# AIP AD 2.0 Parser, Persistencia y Enrichment

Este documento describe la implementación actual del flujo que toma un PDF AIP `AD-2.0`, extrae las secciones necesarias para plan de vuelo, las guarda en MongoDB y opcionalmente las enriquece con LLM.

Estado actual:

```text
ICAO
  -> scraper descarga PDFs ANAC AIP
  -> import service selecciona solo AD-2.0
  -> parser hibrido Docling + PyMuPDF extrae layout/tablas
  -> sectionizer arma internamente AD 2.1 a AD 2.25
  -> table repair corrige tablas core declarativamente
  -> import service filtra a 7 secciones core
  -> aerodrome_repo.upsert guarda AerodromeDocument.current
  -> enrichment opcional procesa solo esas 7 secciones
  -> section.data queda enriquecido y se guarda en MongoDB
```

## Scope Productivo

El producto ahora persiste y enriquece solo las secciones core necesarias para información básica de plan de vuelo:

```python
FLIGHT_PLANNING_AD_SECTION_IDS = (
    "AD 2.1",
    "AD 2.2",
    "AD 2.3",
    "AD 2.4",
    "AD 2.12",
    "AD 2.13",
    "AD 2.19",
)
```

Archivo: `app/schemas/ad_sections.py`

`OPERATIONAL_AD_SECTION_IDS` apunta a ese mismo tuple para mantener compatibilidad con código existente.

Importante:

- El parser sigue detectando `AD 2.1` a `AD 2.25` internamente.
- La validación completa ayuda a detectar PDFs mal segmentados.
- Solo las 7 secciones core llegan a la colección principal `aerodromes`.
- Los artefactos técnicos `raw_extractions` y `pre_llm_sections` pueden conservar más contexto para debug.

## Orquestación

Archivo: `app/services/aerodrome_import_service.py`

Función principal:

```python
async def import_aerodrome_from_aip(
    icao: str,
    output_dir: Path | None = None,
    enrich: bool = True,
) -> AerodromeResponse
```

Flujo:

1. Normaliza el ICAO.
2. Descarga PDFs con `download_aip_pdfs(...)`.
3. Selecciona solo PDFs cuyo nombre contiene `AD-2.0`.
4. Llama a `parse_aerodrome_from_documents(ad20_paths, icao=icao)`.
5. Filtra secciones con `filter_operational_sections(...)`.
6. Persiste con `aerodrome_repo.upsert(...)`.
7. Si `enrich=True`, llama a `enrich_aerodrome_document(...)`.

Importar sin enrichment:

```bash
AIP_PARSER_ENGINE=docling uv run python -m scripts.import_aerodrome_from_aip --icao SAMR --skip-enrichment
```

Importar con enrichment:

```bash
AIP_PARSER_ENGINE=docling uv run python -m scripts.import_aerodrome_from_aip --icao SAMR
```

## Scraper

Archivo: `app/services/scraper/aip_scraper.py`

Función pública:

```python
async def download_aip_pdfs(
    icao: str,
    output_dir: Path | None = None,
    headless: bool = True,
) -> list[Path]
```

Características:

- Está acoplado al sitio AIP ANAC Argentina.
- Usa Playwright/Chromium.
- Descarga secciones PDF configuradas, pero el import service solo usa `AD-2.0`.
- Si falta el PDF requerido, el import service falla con `AipImportError`.

## Parser

Archivo principal: `app/services/scraper/aip_parser.py`

Funciones públicas:

```python
parse_aerodrome_from_ad20(pdf_path: Path) -> AerodromeCreate

parse_aerodrome_from_documents(
    pdf_paths: list[Path],
    icao: str,
    *,
    engine: str | None = None,
) -> AerodromeCreate
```

### Engine

Config:

```bash
AIP_PARSER_ENGINE=docling
```

Engines disponibles:

- `docling`: default productivo actual. Es hibrido `docling+pymupdf`.
- `pymupdf`: fallback funcional sin Docling.

El parser hibrido hace:

1. PyMuPDF extrae texto, headers, bboxes, orden visual y tablas basicas.
2. Docling extrae estructura de tablas.
3. `docling_aip_parser.upgrade_tables_in_layout(...)` reemplaza o inyecta tablas Docling en el layout base de PyMuPDF usando matching por bbox.
4. El layout final queda con `engine="docling+pymupdf"`.

Si Docling falla, se loguea warning y se vuelve al layout PyMuPDF.

### Artifact Interno

El parser produce un layout versionado:

```json
{
  "schema_version": "aip-layout-v1",
  "engine": "docling+pymupdf",
  "source_path": "...",
  "pages": [
    {
      "page": 1,
      "width": 595,
      "height": 842,
      "elements": [
        {
          "type": "text | table",
          "text": "...",
          "page": 1,
          "bbox": [x0, y0, x1, y1],
          "order": 1,
          "table": {
            "label": "...",
            "columns": ["..."],
            "rows": [{"...": "..."}],
            "cells": ["..."],
            "raw_rows": [["..."]]
          }
        }
      ]
    }
  ]
}
```

Ese artefacto se guarda como `raw_extraction` y alimenta el sectionizer.

## Sectionizer

Archivo: `app/services/scraper/aip_segmenter.py`

Función principal:

```python
sectionize_layout_artifact(
    layout_artifact: dict[str, Any],
    icao: str,
    source_path: Path,
    logger: object,
    format_error: type[Exception],
) -> SectionizedLayout
```

Responsabilidades:

- Ordena elementos por pagina, `y`, `x` y `order`.
- Detecta headers `AD 2.x`.
- Agrupa bloques por sección activa.
- Exige que existan las 25 secciones `AD 2.1` a `AD 2.25`.
- Aplica `repair_section_tables(section_id, blocks)`.
- Construye `SectionSchema` por sección.

Cada `SectionSchema` contiene:

```json
{
  "section_id": "AD 2.13",
  "title": "AD 2.13",
  "section_title": "DISTANCIAS DECLARADAS / DECLARED DISTANCES",
  "raw_text": "...",
  "data": {
    "schema_hint": "...",
    "tables": [],
    "quality": {}
  },
  "anchors": {
    "section_blocks": [],
    "source": {}
  }
}
```

## Table Repair

Archivos:

- `app/services/scraper/aip_table_profiles.py`
- `app/services/scraper/aip_table_repair.py`

El repair es deterministico y se ejecuta antes del enrichment. Su objetivo no es extraer semántica aeronáutica final, sino dejar tablas fieles y estables para DB/LLM.

Perfiles activos:

- `AD 2.2`
- `AD 2.3`
- `AD 2.4`
- `AD 2.12`
- `AD 2.13`
- `AD 2.19`

No hay reglas por ICAO. Las reglas son por patrón de sección.

### AD 2.3

Problema observado:

- Docling puede perder la primera fila.
- Puede mezclar `Aduanas`, `Inmigraciones`, sanidad y AIS.

Corrección:

- Perfil con columnas:

```python
[
    "item",
    "Servicio / Service",
    "Horas de funcionamiento / Operational hours",
]
```

- Si el `block.text` contiene filas pipe (`1 | Servicio | Hora`), el repair reconstruye filas desde texto.
- Esto recupera casos como SAEZ donde la tabla Docling omitía `Explotador del AD`.

### AD 2.12

Problema observado:

- ANAC usa dos subtablas:
  - columnas `1-7`: características físicas principales de pista;
  - columnas `8-14`: SWY, CWY, strip, RESA, arresting system, OFZ, remarks.
- Docling/PyMuPDF pueden mezclar la subtabla secundaria dentro de la primera.

Corrección:

- Detecta header secundario por labels como `Dimensiones SWY`, `CWY`, `RESA`, `OFZ`, `Observaciones`.
- Divide en dos tablas:
  - tabla principal de pista;
  - tabla suplementaria por pista.
- Limpia designadores contaminados como `35 Dimensiones` a `35`.
- Hereda dimensiones/resistencia para pista recíproca cuando el PDF omite repetirlas.
- Limpia contaminación como `Ubicación...` dentro de coordenadas.
- Agrega metadata:

```json
{
  "repair_applied": true,
  "repair_profile": "AD 2.12",
  "repair_warnings": ["header_like_row_split_to_continuation_table"]
}
```

### AD 2.13

Problema observado:

- Fila numérica `1 2 3 4 5 6` podía quedar como dato.
- `TODA` y `ASDA` podían quedar pegadas en una celda.
- Remarks bilingües podían partirse entre RWY `18` y `36`.

Corrección:

- Elimina headers numéricos simples o compuestos.
- Separa `TODA` y `ASDA` cuando aparecen como `3.135 2.835`.
- Une remarks bilingües cuando una fila termina en `/` y la siguiente es continuación en inglés.

### AD 2.19

La tabla viene estable en SAEZ, SAME y SAMR. El repair conserva columnas canónicas:

```python
[
    "Tipo de ayuda, MAG VAR,",
    "ID",
    "Frecuencia y Canal / Frequency and channel",
    "Horas de funcionamiento / Hours of operation",
    "COORD GEO del emplazamiento de la antena transmisora / Position of transmitting antenna coordinates",
    "Elevacion de la antena transmisora del DME / Elevation of DME transmitting antenna",
    "Observaciones / Remarks",
]
```

## DTO de Salida del Parser

`parse_aerodrome_from_documents(...)` devuelve `AerodromeCreate`.

Archivo: `app/schemas/aerodrome.py`

Campos principales:

- `icao_code`
- `name`
- `full_name`
- `airac_cycle`
- `airac_effective_date`
- `airac_expiry_date`
- `source_document`
- `downloaded_by`
- `ad_sections`

El parser devuelve 25 secciones. La importación filtra después a 7.

## Persistencia

Archivo: `app/repositories/aerodrome_repo.py`

Función principal:

```python
async def upsert(data: AerodromeCreate) -> AerodromeDocument
```

Validación:

- Deben existir exactamente las secciones de `OPERATIONAL_AD_SECTION_IDS`.
- No puede haber secciones inesperadas.
- No puede haber duplicados.
- Cada sección debe tener `raw_text` no vacío.

Documento principal:

```json
{
  "_id": "SAMR",
  "icao": "SAMR",
  "name": "San Rafael",
  "full_name": "...",
  "current": {
    "ad_sections": [
      {
        "section_id": "AD 2.1",
        "section_title": "...",
        "raw_text": "...",
        "data": {},
        "anchors": {}
      }
    ],
    "_meta": {}
  },
  "history": []
}
```

AIRAC:

- Si cambia `airac_cycle`, el `current` anterior pasa a `history`.
- Si no cambia, se reemplaza `current`.

## Artefactos Intermedios

El parser guarda artefactos si están disponibles:

- `RawExtractionDocument`
- `PreLlmSectionsDocument`

Estas colecciones conservan material técnico de extracción/layout. No son el contrato productivo principal.

`pre_llm_sections` contiene por sección:

- `section_id`
- `title`
- `schema_hint`
- `section_blocks`
- `tables`
- `quality`
- `source`
- `raw_text_preview`

## Enrichment

Archivo: `app/services/enrichment/aerodrome_enricher.py`

Función principal:

```python
async def enrich_aerodrome_document(
    aerodrome_doc: AerodromeDocument,
    section_ids: list[str] | None = None,
    *,
    save: bool = True,
) -> AerodromeDocument
```

Default actual:

```python
TARGET_SECTION_IDS = OPERATIONAL_AD_SECTION_IDS
```

Por lo tanto, el enrichment default procesa solo:

- `AD 2.1`
- `AD 2.2`
- `AD 2.3`
- `AD 2.4`
- `AD 2.12`
- `AD 2.13`
- `AD 2.19`

### Flujo Por Sección

Para cada sección persistida:

1. Resuelve `section_blocks` desde `section.anchors`.
2. Calcula hash de `raw_text`.
3. Si `section.data._extraction.raw_text_sha256` coincide y `status == "ok"`, usa cache y no llama al LLM.
4. Carga schema Pydantic desde `SECTION_SCHEMA_REGISTRY`.
5. Carga contrato opcional con `load_ad2_contract(...)`.
6. Llama a `provider.chat_structured(...)`.
7. Valida payload con Pydantic.
8. Inyecta tablas de layout para secciones tabulares críticas.
9. Postprocesa payload.
10. Escribe:

```json
{
  "...payload enriquecido...": "...",
  "_layout": {
    "version": "v1",
    "source": "section_blocks",
    "section_blocks": []
  },
  "_extraction": {
    "engine": "...",
    "model": "...",
    "prompt_version": "ad2-v1",
    "raw_text_sha256": "...",
    "extracted_at": "...",
    "status": "ok",
    "error": null
  }
}
```

### Layout Override

Para estas secciones, enrichment prefiere tablas reparadas de layout antes que una reconstrucción del LLM:

- `AD 2.12`
- `AD 2.13`
- `AD 2.19`

Esto es intencional: esas tablas son críticas y deben preservar trazabilidad desde el PDF.

## Qué Queda En MongoDB

Después de importar y enriquecer, cada sección en `current.ad_sections` contiene:

- `raw_text`: texto estable para auditoría y fallback.
- `anchors.section_blocks`: bloques con `type`, `text`, `page`, `bbox`, `order` y tabla si aplica.
- `data.fields`: campos extraídos por LLM o normalización determinística.
- `data.tables`: tablas estructuradas.
- `data._layout`: copia de bloques usados por enrichment.
- `data._extraction`: metadata del enrichment.

Ejemplo conceptual:

```json
{
  "section_id": "AD 2.13",
  "section_title": "DISTANCIAS DECLARADAS / DECLARED DISTANCES",
  "raw_text": "AD 2.13 ...",
  "data": {
    "section_id": "AD 2.13",
    "schema": "GenericAd2SectionData",
    "fields": [],
    "tables": [
      {
        "name": "table_1",
        "columns": [
          "Designador RWY / RWY designator",
          "TORA (m)",
          "TODA (m)",
          "ASDA (m)",
          "LDA (m)",
          "Observaciones / Remarks"
        ],
        "rows": []
      }
    ],
    "_layout": {},
    "_extraction": {}
  },
  "anchors": {}
}
```

## Responsabilidad Parser vs Enrichment

Las correcciones de estructura se hacen en parser:

- filas corridas;
- headers numéricos;
- subtablas;
- celdas combinadas;
- columnas pegadas;
- valores de tabla mal alineados.

El enrichment no debe reparar tablas rotas. Su función es convertir material ya estructurado a payload semántico validado.

Esta separación es clave para escalar a cualquier AD ANAC:

- parser repair es determinístico y trazable;
- enrichment queda más simple;
- no se depende del LLM para adivinar tablas.

## Estado Por Sección Core

Con SAEZ, SAME y SAMR:

- `AD 2.1`: estable.
- `AD 2.2`: estable para datos principales.
- `AD 2.3`: mejorado con reconstrucción desde `block.text`; sigue siendo sensible a notas largas y continuaciones.
- `AD 2.4`: estable para combustible/handling.
- `AD 2.12`: mejorado con split de subtablas; es la sección más delicada.
- `AD 2.13`: estable después de normalizar distancias declaradas.
- `AD 2.19`: estable.

## Comandos De Prueba

Tests unitarios:

```bash
uv run pytest tests/unit -q
```

Tests focales del parser:

```bash
uv run pytest tests/unit/test_aip_table_repair.py -q
uv run pytest tests/unit/test_aip_parser.py -q
```

Tests de importación:

```bash
uv run pytest tests/integration/test_aerodrome_import_service.py -q
```

Importar sin enrichment:

```bash
AIP_PARSER_ENGINE=docling uv run python -m scripts.import_aerodrome_from_aip --icao SAMR --skip-enrichment
```

Enriquecer después:

```bash
uv run python -m scripts.enrich_aerodrome --icao SAMR -v
```

Enriquecer una sección:

```bash
uv run python -m scripts.enrich_aerodrome --icao SAMR --sections "AD 2.13" -v
```

## Cómo Validar En MongoDB

Para cada documento en `aerodromes`:

```javascript
db.aerodromes.findOne(
  { _id: "SAMR" },
  { "current.ad_sections.section_id": 1 }
)
```

Debe devolver exactamente:

```json
[
  "AD 2.1",
  "AD 2.2",
  "AD 2.3",
  "AD 2.4",
  "AD 2.12",
  "AD 2.13",
  "AD 2.19"
]
```

Para revisar estado de enrichment:

```javascript
db.aerodromes.findOne(
  { _id: "SAMR" },
  {
    "current.ad_sections.section_id": 1,
    "current.ad_sections.data._extraction.status": 1,
    "current.ad_sections.data._extraction.error": 1
  }
)
```

## Dependencias Relevantes

- `pymupdf`: layout base, texto, bboxes, tablas simples.
- `docling`: estructura avanzada de tablas.
- `pydantic`: validación de schemas de enrichment.
- proveedor LLM configurado por `LLM_PROVIDER`.
- MongoDB/Beanie para persistencia.

## Limitaciones Actuales

- El scraper sigue siendo ANAC Argentina, no mundial.
- El parser está optimizado para PDFs AD-2.0 con texto embebido.
- OCR no es parte del parser principal.
- `AD 2.12` sigue siendo la sección más difícil por subtablas y valores omitidos visualmente.
- Las notas largas de `AD 2.3` pueden requerir ajustes adicionales si se quiere modelarlas semánticamente.
- Los artefactos técnicos pueden contener las 25 secciones aunque el documento productivo solo guarde 7.

## Regla De Evolución

Antes de agregar una nueva sección al scope productivo:

1. Confirmar que el parser la extrae bien en SAEZ, SAME y SAMR.
2. Agregar perfil declarativo si es tabular.
3. Agregar tests unitarios de repair.
4. Agregar test real con PDF local.
5. Recién después incluirla en `FLIGHT_PLANNING_AD_SECTION_IDS`.
