"""LLM provider adapters for structured section extraction."""

from __future__ import annotations

import json
from typing import Any, Protocol

import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
from ollama import Client as OllamaClient
from openai import OpenAI

from app.core.config import Settings, get_settings

_VERTEX_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def _vertex_openapi_base_url(*, project_id: str, location: str) -> str:
    """Build OpenAPI-compatible base URL for Vertex / Agent Platform.

    - ``location=global`` → ``aiplatform.googleapis.com`` (typical for hosted Gemini).
    - Regional locations (e.g. ``us-central1``) → ``{location}-aiplatform.googleapis.com``
      (required for Llama MaaS and similar; matches official curl samples).
    """
    pid = project_id.strip()
    loc = location.strip()
    if loc == "global":
        return (
            f"https://aiplatform.googleapis.com/v1/projects/{pid}"
            "/locations/global/endpoints/openapi"
        )
    return (
        f"https://{loc}-aiplatform.googleapis.com/v1/projects/{pid}"
        f"/locations/{loc}/endpoints/openapi"
    )


class StructuredLlmProvider(Protocol):
    engine_name: str
    model_name: str

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str: ...


def _build_user_prompt(icao: str, section_id: str, raw_text: str) -> str:
    return (
        f"ICAO: {icao}\n"
        f"Section: {section_id}\n"
        "IMPORTANT: Preserve source spelling exactly for literal text values "
        "(accents/diacritics, apostrophes, dashes, punctuation).\n"
        "Extract fields from this raw text:\n\n"
        f"{raw_text}"
    )


def _build_user_prompt_with_blocks(
    icao: str,
    section_id: str,
    raw_text: str,
    section_blocks: list[dict[str, Any]] | None,
) -> str:
    base = _build_user_prompt(icao, section_id, raw_text)
    if not section_blocks:
        return base
    return (
        f"{base}\n\n"
        "Structured section blocks (use these as structural hints for table/list boundaries):\n"
        f"{json.dumps(section_blocks, ensure_ascii=True)}"
    )


def _build_contract_instructions(contract: dict[str, Any] | None) -> str:
    if contract is None:
        return ""
    return (
        "Extraction contract (authoritative):\n"
        f"{json.dumps(contract, ensure_ascii=True)}\n"
        "Return JSON that follows this contract exactly. "
        "Keep documentary field boundaries: do not split a field value into semantic subfields. "
        "Do not paraphrase, translate, summarize, or normalize content beyond exact documentary spelling. "
        "If a value appears as a single row value in source, keep it as a single value string. "
        "When tables exist, preserve column names exactly and output row cells exactly as they appear."
    )


def _build_ollama_messages(
    icao: str,
    section_id: str,
    raw_text: str,
    contract: dict[str, Any] | None,
    section_blocks: list[dict[str, Any]] | None,
) -> list[dict[str, str]]:
    system = (
        "You extract structured aeronautical data from AD 2.0 sections. "
        "Return only JSON that matches the provided schema. "
        "Use null when unknown. Do not invent values. "
        "Preserve orthography from source text, including accents/diacritics and punctuation. "
        "Never truncate official names: if a value includes slash-separated parts (e.g. 'NAME / SUBNAME'), keep all parts. "
        "When a line combines ICAO indicator and aerodrome name (e.g. 'SAXX - NAME / SUBNAME'), keep the full combined text for display fields and keep the full name text after the indicator for name fields."
    )
    contract_text = _build_contract_instructions(contract)
    if contract_text:
        system = f"{system}\n\n{contract_text}"
    return [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": _build_user_prompt_with_blocks(icao, section_id, raw_text, section_blocks),
        },
    ]


def _build_cloud_messages(
    icao: str,
    section_id: str,
    raw_text: str,
    schema: dict[str, Any],
    contract: dict[str, Any] | None,
    section_blocks: list[dict[str, Any]] | None,
) -> list[dict[str, str]]:
    schema_text = json.dumps(schema, ensure_ascii=True)
    system = (
        "You extract structured aeronautical data from bilingual (Spanish/English) "
        "AIP AD 2.0 sections. The text may contain OCR artifacts. "
        "Return ONLY valid JSON matching this schema exactly. "
        "Use null for unknown values. Do not invent data. "
        "Preserve source orthography exactly whenever possible (accents/diacritics, apostrophes, hyphens, symbols). "
        "Never truncate official names: if a value includes slash-separated parts (e.g. 'NAME / SUBNAME'), keep all parts. "
        "When a line combines ICAO indicator and aerodrome name (e.g. 'SAXX - NAME / SUBNAME'), keep the full combined text for display fields and keep the full name text after the indicator for name fields.\n"
        f"SCHEMA:\n{schema_text}"
    )
    contract_text = _build_contract_instructions(contract)
    if contract_text:
        system = f"{system}\n\n{contract_text}"
    return [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": _build_user_prompt_with_blocks(icao, section_id, raw_text, section_blocks),
        },
    ]


class OllamaProvider:
    engine_name = "ollama"

    def __init__(self, settings: Settings) -> None:
        self.model_name = settings.ollama_model
        self._temperature = settings.ollama_temperature
        self._client = OllamaClient(host=settings.ollama_host)

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str:
        response = self._client.chat(
            model=self.model_name,
            messages=_build_ollama_messages(icao, section_id, raw_text, contract, section_blocks),
            format=schema,
            options={"temperature": self._temperature},
        )
        return response.message.content


class _OpenAiCompatibleProvider:
    engine_name = "openai-compatible"

    def __init__(self, *, model_name: str, api_key: str, base_url: str, temperature: float) -> None:
        self.model_name = model_name
        self._temperature = temperature
        self._client = OpenAI(api_key=api_key, base_url=base_url)

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str:
        completion = self._client.chat.completions.create(
            model=self.model_name,
            messages=_build_cloud_messages(
                icao,
                section_id,
                raw_text,
                schema,
                contract,
                section_blocks,
            ),
            response_format={"type": "json_object"},
            temperature=self._temperature,
        )
        content = completion.choices[0].message.content
        if not content:
            raise ValueError(f"Empty response from provider model={self.model_name}")
        return content


class OpenRouterProvider(_OpenAiCompatibleProvider):
    engine_name = "openrouter"

    def __init__(self, settings: Settings) -> None:
        if not settings.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY is required when LLM_PROVIDER=openrouter")
        super().__init__(
            model_name=settings.openrouter_model,
            api_key=settings.openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=settings.llm_temperature,
        )


class GroqProvider(_OpenAiCompatibleProvider):
    engine_name = "groq"

    def __init__(self, settings: Settings) -> None:
        if not settings.groq_api_key:
            raise ValueError("GROQ_API_KEY is required when LLM_PROVIDER=groq")
        super().__init__(
            model_name=settings.groq_model,
            api_key=settings.groq_api_key,
            base_url="https://api.groq.com/openai/v1",
            temperature=settings.llm_temperature,
        )


class NvidiaProvider(_OpenAiCompatibleProvider):
    engine_name = "nvidia"

    def __init__(self, settings: Settings) -> None:
        if not settings.nvidia_api_key:
            raise ValueError("NVIDIA_API_KEY is required when LLM_PROVIDER=nvidia")
        super().__init__(
            model_name=settings.nvidia_model,
            api_key=settings.nvidia_api_key,
            base_url="https://integrate.api.nvidia.com/v1",
            temperature=settings.llm_temperature,
        )

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str:
        # NVIDIA endpoint can reject response_format for some models/routes.
        # We enforce JSON in prompt and validate with Pydantic downstream.
        completion = self._client.chat.completions.create(
            model=self.model_name,
            messages=_build_cloud_messages(
                icao,
                section_id,
                raw_text,
                schema,
                contract,
                section_blocks,
            ),
            temperature=self._temperature,
        )
        content = completion.choices[0].message.content
        if not content:
            raise ValueError(f"Empty response from provider model={self.model_name}")
        return content


class GoogleAiStudioProvider(_OpenAiCompatibleProvider):
    engine_name = "google_ai_studio"

    def __init__(self, settings: Settings) -> None:
        if not settings.google_ai_studio_api_key:
            raise ValueError(
                "GOOGLE_AI_STUDIO_API_KEY is required when LLM_PROVIDER=google_ai_studio"
            )
        super().__init__(
            model_name=settings.google_ai_studio_model,
            api_key=settings.google_ai_studio_api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            temperature=settings.llm_temperature,
        )

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str:
        # AI Studio OpenAI-compatible endpoint can vary in response_format support.
        # We enforce JSON in prompt and validate with Pydantic downstream.
        completion = self._client.chat.completions.create(
            model=self.model_name,
            messages=_build_cloud_messages(
                icao,
                section_id,
                raw_text,
                schema,
                contract,
                section_blocks,
            ),
            temperature=self._temperature,
        )
        content = completion.choices[0].message.content
        if not content:
            raise ValueError(f"Empty response from provider model={self.model_name}")
        return content


class GoogleVertexProvider:
    """Gemini / MaaS via Vertex AI OpenAPI shim (OAuth2; no plain API keys).

    Use ``GOOGLE_VERTEX_LOCATION=us-central1`` (or the region from the model card)
    for MaaS models like ``meta/llama-3.3-70b-instruct-maas`` — they do not use the
    ``global`` endpoint / host.
    """

    engine_name = "google_vertex"

    def __init__(self, settings: Settings) -> None:
        if not settings.google_vertex_project_id:
            raise ValueError(
                "GOOGLE_VERTEX_PROJECT_ID is required when LLM_PROVIDER=google_vertex"
            )
        self.model_name = settings.google_vertex_model.strip()
        self._temperature = settings.llm_temperature
        self._base_url = _vertex_openapi_base_url(
            project_id=settings.google_vertex_project_id,
            location=settings.google_vertex_location,
        )
        if settings.google_vertex_credentials_file:
            self._credentials = service_account.Credentials.from_service_account_file(
                settings.google_vertex_credentials_file,
                scopes=_VERTEX_SCOPES,
            )
        else:
            self._credentials, _ = google.auth.default(scopes=_VERTEX_SCOPES)

    def _get_client(self) -> OpenAI:
        if not self._credentials.valid:
            self._credentials.refresh(google.auth.transport.requests.Request())
        return OpenAI(api_key=self._credentials.token, base_url=self._base_url)

    def chat_structured(
        self,
        *,
        icao: str,
        section_id: str,
        raw_text: str,
        schema: dict[str, Any],
        contract: dict[str, Any] | None = None,
        section_blocks: list[dict[str, Any]] | None = None,
    ) -> str:
        client = self._get_client()
        messages = _build_cloud_messages(icao, section_id, raw_text, schema, contract, section_blocks)
        kwargs: dict[str, Any] = {
            "model": self.model_name,
            "messages": messages,
            "temperature": self._temperature,
        }
        # Hosted Gemini usually accepts OpenAI json_object; open MaaS models often do not.
        if not self.model_name.startswith("meta/"):
            kwargs["response_format"] = {"type": "json_object"}

        completion = client.chat.completions.create(**kwargs)
        content = completion.choices[0].message.content
        if not content:
            raise ValueError(f"Empty response from provider model={self.model_name}")
        return content


def get_llm_provider() -> StructuredLlmProvider:
    settings = get_settings()
    if settings.llm_provider == "openrouter":
        return OpenRouterProvider(settings)
    if settings.llm_provider == "groq":
        return GroqProvider(settings)
    if settings.llm_provider == "nvidia":
        return NvidiaProvider(settings)
    if settings.llm_provider == "google_ai_studio":
        return GoogleAiStudioProvider(settings)
    if settings.llm_provider == "google_vertex":
        return GoogleVertexProvider(settings)
    return OllamaProvider(settings)
