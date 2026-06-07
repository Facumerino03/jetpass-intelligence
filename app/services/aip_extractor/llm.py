"""Vertex AI Gemini client for structured AIP section extraction."""

import json
import os
from functools import lru_cache
from typing import TypeVar

from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, ValidationError

load_dotenv()

SYSTEM_PROMPT = """Sos un experto en documentación aeronáutica AIP OACI.
Extraé únicamente los datos presentes en el texto de la sección proporcionada.
No inventes valores ni infieras datos que no estén explícitos.
Respondé solo con JSON válido según el schema indicado.

Idioma de los valores (MUY IMPORTANTE):
Los documentos AIP argentinos incluyen traducciones en el formato
"texto en español / text in English". Conservá únicamente la parte en español.

Para decidir si una barra es un separador bilingüe o parte del contenido, aplicá
estas condiciones — deben cumplirse TODAS para que sea bilingüe:
  1. La barra tiene espacio antes Y después: " / "
  2. El lado derecho es claramente una traducción al inglés del lado izquierdo
     (tienen el mismo significado semántico, no son datos diferentes)
  3. El resultado de quedarte solo con el lado izquierdo tiene sentido completo

Si alguna condición no se cumple, el valor NO es bilingüe y se conserva entero.

Ejemplos bilingües (se corta):
  "Días hábiles 11:00-20:00 UTC / Working days 11:00-20:00 UTC" → "Días hábiles 11:00-20:00 UTC"
  "EZEIZA TORRE / EZEIZA TOWER" → "EZEIZA TORRE"
  "Centro geométrico de pista 11/29 / Geometric center of runway 11/29" → "Centro geométrico de pista 11/29"

Ejemplos que NO se cortan (barra técnica o condición 2 no cumplida):
  "O/R"         → "O/R"        (no es una traducción, es una sigla)
  "IFR/VFR"     → "IFR/VFR"    (sigla técnica)
  "82/R/B/W/T"  → "82/R/B/W/T" (código PCN)
  "11/29"       → "11/29"      (par de pista)
  "28.4°C - 8.4°C" → "28.4°C - 8.4°C" (no hay barra, el guión es separador numérico)

Sin espacios, solo se cortan traducciones evidentes de una sola palabra:
  "Sí/Yes" → "Sí"
  "No/No"  → "No"

Regla de oro: si tenés dudas, no cortés. Es mejor conservar texto de más que perder información.

Reglas para valores vacíos y errores (MUY IMPORTANTE):

1. Si el documento dice "NIL" (No Item Listed / nada que reportar) → devolvé null.
   Nunca devuelvas el string "NIL" en el JSON.

2. Si el dato no aparece en el texto, no aplica, o la celda está vacía → devolvé null.

3. Si el texto está ilegible, corrupto por extracción PDF, o no podés determinar
   el valor con certeza por formato roto → devolvé exactamente el string
   "ERROR_EXTRACCION" en ESE campo. NUNCA uses null para un fallo de extracción.

4. "NU" (Not Usable / No Utilizable) en distancias declaradas NO es NIL:
   seguí las instrucciones del schema (ej. lda_not_usable).

Resumen: null = dato ausente o NIL en el AIP; ERROR_EXTRACCION = fallo al leer."""

T = TypeVar("T", bound=BaseModel)


def _get_model_name() -> str:
    return os.getenv("VERTEX_MODEL", "gemini-2.0-flash-001")


@lru_cache(maxsize=1)
def _get_client() -> genai.Client:
    project = os.getenv("GOOGLE_CLOUD_PROJECT")
    location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    if not project:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT is required. Set it in .env or environment."
        )
    return genai.Client(vertexai=True, project=project, location=location)


def _parse_response(
    response: types.GenerateContentResponse,
    schema_model: type[T],
) -> T:
    """Parse generate_content response into a validated Pydantic model."""
    if response.parsed is not None:
        if isinstance(response.parsed, schema_model):
            return response.parsed
        return schema_model.model_validate(response.parsed)

    raw = response.text
    if not raw:
        raise ValueError("Empty response from model")
    return schema_model.model_validate_json(raw)


def extract_section(
    section_id: str,
    section_text: str,
    schema_model: type[T],
    *,
    max_retries: int = 1,
) -> T:
    """
    Send section text to Gemini with structured JSON output and validate.

    Retries once on validation failure, passing the error as extra context.
    """
    client = _get_client()
    model = _get_model_name()

    user_prompt = (
        f"Sección: {section_id}\n\n"
        f"Texto de la sección AIP:\n\n{section_text}"
    )

    last_error: str | None = None
    attempts = max_retries + 1

    for attempt in range(attempts):
        contents = user_prompt
        system_instruction = SYSTEM_PROMPT
        if last_error:
            contents = (
                f"{user_prompt}\n\n"
                f"La respuesta anterior no pasó validación: {last_error}. "
                "Corregí el JSON para cumplir el schema exactamente."
            )

        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json",
                response_schema=schema_model,
            ),
        )

        try:
            return _parse_response(response, schema_model)
        except (ValidationError, ValueError) as exc:
            last_error = str(exc)
            if attempt == attempts - 1:
                raise ValueError(
                    f"Failed to validate LLM output for {section_id}: {last_error}"
                ) from exc

    raise RuntimeError(f"Unexpected extraction failure for {section_id}")


def extract_section_as_dict(
    section_id: str,
    section_text: str,
    schema_model: type[BaseModel],
) -> dict:
    """Extract section and return as a plain dict (JSON-serializable)."""
    result = extract_section(section_id, section_text, schema_model)
    return json.loads(result.model_dump_json())
