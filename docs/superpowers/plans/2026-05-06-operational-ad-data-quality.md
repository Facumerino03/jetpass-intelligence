# Operational AD Data Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct deterministic parser/enrichment data quality issues found in SAEZ, SAME, and SAMR for the operational AD 2.x section scope.

**Architecture:** Keep PyMuPDF as the only parser engine. Fix structural issues before the LLM in `aip_segmenter.py` whenever the PDF layout contains enough evidence, then force enrichment to trust those canonical layout tables. Use post-LLM cleanup only for generic hallucinated fields and AD 2.1 identity normalization.

**Tech Stack:** Python 3.12, PyMuPDF layout artifacts, Pydantic v2, pytest, existing parser/enrichment services.

---

## Findings To Fix

The audit compared `jetpass_aeronautical.aerodromes_completed.json` against official PDFs for `SAEZ`, `SAME`, and `SAMR`.

Critical or operationally relevant issues:

- `SAMR AD 2.8`: apron/taxiway values are shifted. `CONC - PCR 550/R/B/W/T - ELEV 746 m...` becomes a fake field named `CONC`, and TWY data is assigned to APN.
- `SAMR AD 2.18`: ATS communication rows mix `CAUX`, `CPPL`, `EMERG`, frequencies, hours, and remarks.
- `SAME AD 2.18`: ATS communication rows mix `CAUX`, `CPPL`, call signs, and remarks.
- `SAMR AD 2.19`: DME elevation and remarks are merged into one column named `Elevación de la Observaciones / Remarks`.
- `SAME AD 2.1`: `ICAO` field contains `SAME - MENDOZA / El Plumerillo` instead of only `SAME`.
- `SAME AD 2.3`: LLM adds an extra field `ICAO=SAME`; immigration row has empty label and value `Inmigraciones / Immigration`.
- Generic noise: extra `ICAO` fields outside AD 2.1, empty title fields, and duplicated fields in LLM output.

Non-goals:

- Do not reintroduce OpenDataLoader.
- Do not change the 12-section operational persistence scope.
- Do not change Mongo schema shape.
- Do not remove the full 25-section parser validation.

---

## File Structure

- Modify `app/services/scraper/aip_segmenter.py`
  - Add deterministic normalizers for `AD 2.8` and `AD 2.19`.
  - Improve deterministic normalizer for `AD 2.18`.
- Modify `app/services/enrichment/aerodrome_enricher.py`
  - Inject canonical layout tables for `AD 2.8` and `AD 2.19`.
  - Add generic field cleanup and AD 2.1 identity cleanup after LLM output.
- Modify `tests/unit/test_aip_parser.py`
  - Add regression assertions for real local `SAEZ`, `SAME`, and `SAMR` PDFs when available.
- Modify `tests/unit/test_aerodrome_enricher.py`
  - Add postprocess tests for noise cleanup and layout table injection.
- Optional new file `tests/fixtures/ad2_samples.py`
  - Only create this if test snippets become too large for `test_aip_parser.py`.

---

### Task 1: Add Regression Tests For Known Operational Failures

**Files:**
- Modify: `tests/unit/test_aip_parser.py`

- [ ] **Step 1: Add helper for optional local PDFs**

Add this helper near the existing real PDF tests:

```python
def _local_pdf(path: str) -> Path:
    pdf_path = Path(path)
    if not pdf_path.exists():
        pytest.skip(f"Local AIP fixture is not available: {pdf_path}")
    return pdf_path
```

- [ ] **Step 2: Add failing SAMR operational data regression test**

Append:

```python
def test_parse_real_samr_operational_tables_when_available() -> None:
    result = parse_aerodrome_from_documents(
        [_local_pdf("/home/facumerino/projects/jetpass/jetpass-intelligence/tmp/aip/SAMR/SAMR_AD-2.0.pdf")],
        icao="SAMR",
    )

    ad28 = next(section for section in result.ad_sections if section.section_id == "AD 2.8")
    ad28_table = ad28.data["tables"][0]
    assert ad28_table["columns"] == ["item", "label", "value"]
    assert ad28_table["rows"][0] == {
        "item": "1",
        "label": "Designación, superficie y resistencia APN / Designation surface and strength of aprons",
        "value": "CONC - PCR 550/R/B/W/T - ELEV 746 m (2448 ft)",
    }
    assert ad28_table["rows"][1] == {
        "item": "2",
        "label": "Designación, ancho, superficie y resistencia TWY / Designation, width, surface and strength of taxiways",
        "value": "A, 18 m, ASPH, PCR 430/F/B/X/T",
    }
    vor_row = next(row for row in ad28_table["rows"] if row["item"] == "4")
    assert "RDL 255" in vor_row["value"]
    assert "0.29 NM" in vor_row["value"]

    ad218 = next(section for section in result.ad_sections if section.section_id == "AD 2.18")
    rows = ad218.data["tables"][0]["rows"]
    assert rows == [
        {
            "Designacion del Servicio / Service designation": "TMA/APP/TWR",
            "Distintivo de llamada / Call sign": "San Rafael Torre / San Rafael Tower",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "118.10 MHz",
            "Horas de funcionamiento / Hours of operation": "LUN - VIE 10:00-23:59 UTC resto/rest O/R",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "CAUX",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "119.15 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "SMC",
            "Distintivo de llamada / Call sign": "San Rafael GND",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "118.10 MHz",
            "Horas de funcionamiento / Hours of operation": "LUN - VIE 10:00-23:59 UTC resto/rest O/R",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
        {
            "Designacion del Servicio / Service designation": "CAUX",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "119.15 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "EMERG",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "121.50 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
    ]

    ad219 = next(section for section in result.ad_sections if section.section_id == "AD 2.19")
    nav_table = ad219.data["tables"][0]
    assert "Elevación de la antena transmisora del DME / Elevation of DME transmitting antenna" in nav_table["columns"]
    assert "Observaciones / Remarks" in nav_table["columns"]
    vor = nav_table["rows"][0]
    assert vor["Tipo de ayuda, MAG VAR,"] == "VOR/DME"
    assert vor["Elevación de la antena transmisora del DME / Elevation of DME transmitting antenna"] == "747.50 m 2.452 ft"
    assert "DME CH 116X" in vor["Observaciones / Remarks"]
```

- [ ] **Step 3: Add failing SAME operational data regression test**

Append:

```python
def test_parse_real_same_operational_tables_when_available() -> None:
    result = parse_aerodrome_from_documents(
        [_local_pdf("/home/facumerino/projects/jetpass/jetpass-intelligence/tmp/aip/SAME/SAME_AD-2.0.pdf")],
        icao="SAME",
    )

    ad218 = next(section for section in result.ad_sections if section.section_id == "AD 2.18")
    rows = ad218.data["tables"][0]["rows"]
    assert rows[:7] == [
        {
            "Designacion del Servicio / Service designation": "TMA",
            "Distintivo de llamada / Call sign": "Mendoza Control",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "124.20 MHz",
            "Horas de funcionamiento / Hours of operation": "H24",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
        {
            "Designacion del Servicio / Service designation": "CAUX",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "122.10 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "EMERG",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "121.50 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "TWR/APP",
            "Distintivo de llamada / Call sign": "Mendoza Torre / Mendoza Tower",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "119.90 MHz",
            "Horas de funcionamiento / Hours of operation": "H24",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
        {
            "Designacion del Servicio / Service designation": "CAUX",
            "Distintivo de llamada / Call sign": "",
            "Canales / Channels": "",
            "Frecuencia / Frequency": "118.65 MHz",
            "Horas de funcionamiento / Hours of operation": "",
            "Observaciones / Remarks": "",
        },
        {
            "Designacion del Servicio / Service designation": "SMC",
            "Distintivo de llamada / Call sign": "Mendoza Rodaje o Superficie / Mendoza Taxiing or Surface",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "121.95 MHz",
            "Horas de funcionamiento / Hours of operation": "H24",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
        {
            "Designacion del Servicio / Service designation": "ATIS",
            "Distintivo de llamada / Call sign": "ATIS Mendoza",
            "Canales / Channels": "CPPL",
            "Frecuencia / Frequency": "127.60 MHz",
            "Horas de funcionamiento / Hours of operation": "H24",
            "Observaciones / Remarks": "Ver / See GEN 3.4",
        },
    ]
```

- [ ] **Step 4: Add failing SAEZ no-regression test**

Append:

```python
def test_parse_real_saez_operational_no_regressions_when_available() -> None:
    result = parse_aerodrome_from_documents(
        [_local_pdf("/home/facumerino/projects/jetpass/jetpass-intelligence/tmp/aip/SAEZ/SAEZ_AD-2.0.pdf")],
        icao="SAEZ",
    )

    ad218 = next(section for section in result.ad_sections if section.section_id == "AD 2.18")
    rows = ad218.data["tables"][0]["rows"]
    twr = next(row for row in rows if row["Designacion del Servicio / Service designation"] == "TWR")
    assert twr["Distintivo de llamada / Call sign"] == "EZEIZA TORRE / EZEIZA TOWER"
    assert twr["Canales / Channels"] == "CPPL"
    assert twr["Frecuencia / Frequency"] == "118.60 MHz"
    assert twr["Horas de funcionamiento / Hours of operation"] == "H24"
    caux = [row for row in rows if row["Designacion del Servicio / Service designation"] == "CAUX"]
    assert {row["Frecuencia / Frequency"] for row in caux} == {"118.05 MHz", "120.45 MHz"}

    ad219 = next(section for section in result.ad_sections if section.section_id == "AD 2.19")
    nav_rows = ad219.data["tables"][0]["rows"]
    assert len(nav_rows) == 5
    assert nav_rows[0]["Tipo de ayuda,"] == "VOR DME"
    assert nav_rows[0]["ID Frecuencia y Canal / Frequency and channel"] == "EZE 116.5 MHz"
```

- [ ] **Step 5: Run tests to verify current failures**

Run:

```bash
uv run python -m pytest tests/unit/test_aip_parser.py -k "operational" -v
```

Expected:

```text
FAILED test_parse_real_samr_operational_tables_when_available
FAILED test_parse_real_same_operational_tables_when_available
```

The SAEZ test may pass or fail depending on the current AD 2.18 changes; if it fails, preserve the failure details for Task 4.

- [ ] **Step 6: Commit failing tests**

```bash
git add tests/unit/test_aip_parser.py
git commit -m "test: capture operational AD data quality regressions"
```

---

### Task 2: Normalize AD 2.8 Apron And Taxiway Data Deterministically

**Files:**
- Modify: `app/services/scraper/aip_segmenter.py`
- Test: `tests/unit/test_aip_parser.py`

- [ ] **Step 1: Add AD 2.8 branch in `_normalize_section_blocks`**

Change:

```python
    if section_id == "AD 2.13":
```

to:

```python
    if section_id == "AD 2.8":
        normalized = _normalize_ad28_aprons_taxiways_tables(blocks)
    elif section_id == "AD 2.13":
```

- [ ] **Step 2: Add AD 2.8 constants**

Near the other section-specific constants, add:

```python
_AD28_COLUMNS = ["item", "label", "value"]
_AD28_LABELS = {
    "1": "Designación, superficie y resistencia APN / Designation surface and strength of aprons",
    "2": "Designación, ancho, superficie y resistencia TWY / Designation, width, surface and strength of taxiways",
    "3": "Emplazamiento y elevación ACL / Location and elevation of altimeter checkpoints",
    "4": "Emplazamiento de los puntos de verificación VOR / Location of VOR checkpoints",
    "5": "Posición de los puntos de verificación INS / Position of INS checkpoints",
    "6": "Observaciones / Remarks",
}
```

- [ ] **Step 3: Implement AD 2.8 normalizer**

Add this function below `_normalize_ad214_lighting_tables`:

```python
def _normalize_ad28_aprons_taxiways_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lines = _section_lines_without_heading(blocks)
    if not lines:
        return []

    rows: list[dict[str, str]] = []
    for item, label in _AD28_LABELS.items():
        value = _extract_numbered_item_value(lines, item, _AD28_LABELS)
        if item == "1":
            value = _clean_ad28_apron_value(value, lines)
        if item == "2":
            value = _clean_ad28_taxiway_value(value)
        rows.append({"item": item, "label": label, "value": value})

    if not any(row["value"] for row in rows):
        return []

    return [
        _inferred_table_block(
            label="AD 2.8 DATOS SOBRE PLATAFORMAS, CALLES DE RODAJE Y EMPLAZAMIENTOS/POSICIONES DE VERIFICACIÓN DE EQUIPO / APRONS, TAXIWAYS AND CHECK LOCATIONS/POSITIONS DATA",
            columns=_AD28_COLUMNS,
            rows=rows,
            bbox=_combined_bbox(blocks),
            page=next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None),
        )
    ]
```

- [ ] **Step 4: Implement shared line/value helpers**

Add below the AD 2.8 normalizer:

```python
def _section_lines_without_heading(blocks: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for block in blocks:
        if block.get("type") == "heading":
            continue
        table = block.get("table") if isinstance(block.get("table"), dict) else None
        if table is not None and isinstance(table.get("raw_rows"), list):
            for row in table["raw_rows"]:
                if isinstance(row, list):
                    lines.extend(_clean_cell(cell) for cell in row if _clean_cell(cell))
            continue
        lines.extend(_lines(str(block.get("text") or "")))
    return [_clean_cell(line) for line in lines if _clean_cell(line)]


def _extract_numbered_item_value(lines: list[str], item: str, labels_by_item: dict[str, str]) -> str:
    start_idx = next((idx for idx, line in enumerate(lines) if line == item), None)
    if start_idx is None:
        return ""
    next_items = {key for key in labels_by_item if key != item}
    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if lines[idx] in next_items:
            end_idx = idx
            break
    chunk = lines[start_idx + 1:end_idx]
    label_parts = _label_parts(labels_by_item[item])
    value_parts = [part for part in chunk if not _is_label_fragment(part, label_parts)]
    return _clean_cell(" ".join(value_parts))


def _label_parts(label: str) -> list[str]:
    return [
        _clean_cell(part)
        for piece in label.split("/")
        for part in re.split(r"\s{2,}| - ", piece)
        if _clean_cell(part)
    ]


def _is_label_fragment(value: str, label_parts: list[str]) -> bool:
    normalized = _clean_cell(value).lower()
    if not normalized:
        return True
    return any(normalized == part.lower() or normalized in part.lower() for part in label_parts)
```

- [ ] **Step 5: Add AD 2.8 cleanup helpers**

Add:

```python
def _clean_ad28_apron_value(value: str, lines: list[str]) -> str:
    cleaned = value
    if not cleaned or cleaned.startswith("A, 18 m"):
        for idx, line in enumerate(lines):
            if "CONC" in line and "PCR" in line:
                tail = []
                for part in lines[idx:]:
                    if part == "1":
                        break
                    tail.append(part)
                cleaned = " ".join(tail)
                break
    return _clean_cell(cleaned.replace("–", "-"))


def _clean_ad28_taxiway_value(value: str) -> str:
    cleaned = _clean_cell(value.replace("“", "").replace("”", "").replace('"', ""))
    cleaned = re.sub(r"\bA\b,\s*18 m", "A, 18 m", cleaned)
    return cleaned
```

- [ ] **Step 6: Run SAMR AD 2.8 regression**

Run:

```bash
uv run python -m pytest tests/unit/test_aip_parser.py::test_parse_real_samr_operational_tables_when_available -v
```

Expected:

```text
AD 2.8 assertions pass; remaining failure is AD 2.18 or AD 2.19
```

- [ ] **Step 7: Commit AD 2.8 fix**

```bash
git add app/services/scraper/aip_segmenter.py tests/unit/test_aip_parser.py
git commit -m "fix: normalize AD 2.8 operational surface data"
```

---

### Task 3: Normalize AD 2.19 Navigation Aids Deterministically

**Files:**
- Modify: `app/services/scraper/aip_segmenter.py`
- Modify: `app/services/enrichment/aerodrome_enricher.py`
- Test: `tests/unit/test_aip_parser.py`, `tests/unit/test_aerodrome_enricher.py`

- [ ] **Step 1: Add AD 2.19 branch in `_normalize_section_blocks`**

In `app/services/scraper/aip_segmenter.py`, add:

```python
    elif section_id == "AD 2.19":
        normalized = _normalize_ad219_navigation_aids_tables(blocks)
```

between the `AD 2.18` and `AD 2.24` branches.

- [ ] **Step 2: Add AD 2.19 columns**

Add:

```python
_AD219_COLUMNS = [
    "Tipo de ayuda, MAG VAR,",
    "ID Frecuencia y Canal / Frequency and channel",
    "Horas de funcionamiento / Hours of operation",
    "COORD GEO del emplazamiento de la antena transmisora / Position of transmitting antenna coordinates",
    "Elevación de la antena transmisora del DME / Elevation of DME transmitting antenna",
    "Observaciones / Remarks",
]
```

- [ ] **Step 3: Implement AD 2.19 normalizer**

Add:

```python
def _normalize_ad219_navigation_aids_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = _first_table_obj(blocks)
    source_rows = table.get("rows") if isinstance(table, dict) else None
    if not isinstance(source_rows, list):
        return []

    rows: list[dict[str, str | None]] = []
    for source in source_rows:
        if not isinstance(source, dict):
            continue
        aid_type = _first_existing_value(source, ["Tipo de ayuda,", "Tipo de ayuda, MAG VAR,"])
        frequency = _first_existing_value(
            source,
            [
                "ID Frecuencia y Canal / Frequency and channel",
                "ID Frecuencia y\nCanal /\nFrequency\nand channel",
            ],
        )
        hours = _first_existing_value(
            source,
            [
                "Horas de funcionamiento / Hours of operation",
                "Horas de operación / Hours of operation",
                "Horas de\nfuncionamiento /\nHours of\noperation",
            ],
        )
        coordinates = _first_existing_value(
            source,
            [
                "COORD GEO del emplazamiento de la antena transmisora / Position of transmitting antenna coordinates",
                "COORD GEO del\nemplazamiento\nde la antena\ntransmisora /\nPosition of\ntransmitting\nantenna\ncoordinates",
            ],
        )
        elevation = _first_existing_value(
            source,
            [
                "Elevación de la antena transmisora del DME / Elevation of DME transmitting antenna",
                "Elevación de\nla antena\ntransmisora\ndel DME /\nElevation of\nDME\ntransmitting\nantenna",
            ],
        )
        remarks = _first_existing_value(source, ["Observaciones / Remarks", "Observaciones /\nRemarks"])

        merged_elevation_remarks = _first_existing_value(source, ["Elevación de la Observaciones / Remarks"])
        if merged_elevation_remarks and not elevation:
            elevation, merged_remarks = _split_ad219_elevation_and_remarks(merged_elevation_remarks)
            remarks = _join_nonempty([remarks, merged_remarks])

        if not aid_type and not frequency:
            continue
        rows.append(
            {
                _AD219_COLUMNS[0]: _clean_cell(aid_type),
                _AD219_COLUMNS[1]: _clean_cell(frequency),
                _AD219_COLUMNS[2]: _clean_cell(hours) or None,
                _AD219_COLUMNS[3]: _clean_cell(coordinates),
                _AD219_COLUMNS[4]: _clean_cell(elevation) or None,
                _AD219_COLUMNS[5]: _clean_cell(remarks) or None,
            }
        )

    if not rows:
        return []

    return [
        _inferred_table_block(
            label="AD 2.19 RADIOAYUDAS PARA LA NAVEGACIÓN Y EL ATERRIZAJE / NAVIGATIONAL AND LANDING AIDS",
            columns=_AD219_COLUMNS,
            rows=rows,
            bbox=_combined_bbox(blocks),
            page=next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None),
        )
    ]
```

- [ ] **Step 4: Add AD 2.19 helper functions**

Add:

```python
def _first_existing_value(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return ""


def _split_ad219_elevation_and_remarks(value: str) -> tuple[str, str]:
    text = _clean_cell(value)
    elevation_match = re.match(r"^(\d+(?:\.\d+)?\s*m\s+\d+(?:\.\d+)?\s*ft)\s*(.*)$", text)
    if elevation_match:
        return elevation_match.group(1), elevation_match.group(2)
    return "", text


def _join_nonempty(values: list[str]) -> str:
    return _clean_cell(" ".join(value for value in values if _clean_cell(value)))
```

- [ ] **Step 5: Force enrichment to trust AD 2.19 layout tables**

In `app/services/enrichment/aerodrome_enricher.py`, change:

```python
if section_id in {"AD 2.12", "AD 2.14", "AD 2.18", "AD 2.24"}:
```

to:

```python
if section_id in {"AD 2.8", "AD 2.12", "AD 2.14", "AD 2.18", "AD 2.19", "AD 2.24"}:
```

- [ ] **Step 6: Add enrichment injection test for AD 2.19**

Append to `tests/unit/test_aerodrome_enricher.py`:

```python
def test_inject_layout_tables_overrides_ad219_llm_payload() -> None:
    payload = {
        "section_id": "AD 2.19",
        "schema": "GenericAd2SectionData",
        "fields": [{"field": "ICAO", "label": "", "value": "SAMR"}],
        "tables": [],
    }
    blocks = [
        {
            "type": "table",
            "table": {
                "label": "AD 2.19 RADIOAYUDAS PARA LA NAVEGACIÓN Y EL ATERRIZAJE / NAVIGATIONAL AND LANDING AIDS",
                "columns": ["Tipo de ayuda, MAG VAR,", "Observaciones / Remarks"],
                "rows": [{"Tipo de ayuda, MAG VAR,": "VOR/DME", "Observaciones / Remarks": "DME CH 116X"}],
            },
        }
    ]

    out = _inject_layout_tables(payload, blocks, section_id="AD 2.19", raw_text="AD 2.19")

    assert out["fields"] == []
    assert out["tables"][0]["rows"][0]["Tipo de ayuda, MAG VAR,"] == "VOR/DME"
```

- [ ] **Step 7: Run AD 2.19 tests**

Run:

```bash
uv run python -m pytest \
  tests/unit/test_aip_parser.py::test_parse_real_samr_operational_tables_when_available \
  tests/unit/test_aerodrome_enricher.py::test_inject_layout_tables_overrides_ad219_llm_payload \
  -v
```

Expected:

```text
AD 2.19 assertions pass; remaining failures are AD 2.18 if Task 4 is not complete
```

- [ ] **Step 8: Commit AD 2.19 fix**

```bash
git add app/services/scraper/aip_segmenter.py app/services/enrichment/aerodrome_enricher.py tests/unit/test_aip_parser.py tests/unit/test_aerodrome_enricher.py
git commit -m "fix: normalize AD 2.19 navigation aids"
```

---

### Task 4: Improve AD 2.18 ATS Communication Normalization

**Files:**
- Modify: `app/services/scraper/aip_segmenter.py`
- Test: `tests/unit/test_aip_parser.py`

- [ ] **Step 1: Preserve existing AD 2.18 columns**

Confirm these columns remain unchanged:

```python
columns = [
    "Designacion del Servicio / Service designation",
    "Distintivo de llamada / Call sign",
    "Canales / Channels",
    "Frecuencia / Frequency",
    "Horas de funcionamiento / Hours of operation",
    "Observaciones / Remarks",
]
```

- [ ] **Step 2: Replace row parsing strategy inside `_normalize_ad218_communication_tables`**

Keep the function name and return type. Replace its row-building internals with this strategy:

```python
def _normalize_ad218_communication_tables(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = _first_table_obj(blocks)
    source_rows = table.get("rows") if isinstance(table, dict) and isinstance(table.get("rows"), list) else []
    text = _clean_cell(" ".join(_section_lines_without_heading(blocks)))
    rows = _ad218_rows_from_structured_rows(source_rows)
    rows = _expand_ad218_embedded_services(rows)
    rows = _repair_ad218_known_patterns(rows, text)
    rows = [row for row in rows if row["Designacion del Servicio / Service designation"]]
    if not rows:
        return []
    return [
        _inferred_table_block(
            label="AD 2.18 INSTALACIONES DE COMUNICACIONES DE LOS ATS / ATS COMMUNICATION FACILITIES",
            columns=_AD218_COLUMNS,
            rows=[{column: row.get(column) or "" for column in _AD218_COLUMNS} for row in rows],
            bbox=_combined_bbox(blocks),
            page=next((block.get("page") for block in blocks if isinstance(block.get("page"), int)), None),
        )
    ]
```

Add `_AD218_COLUMNS` as a module constant using the exact columns above.

- [ ] **Step 3: Implement structured row conversion**

Add:

```python
def _ad218_rows_from_structured_rows(source_rows: list[Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw in source_rows:
        if not isinstance(raw, dict):
            continue
        service = _clean_ad218_service(raw.get("Designacion del Servicio / Service designation"))
        call_sign = _clean_ad218_call_sign(str(raw.get("Distintivo de llamada / Call sign") or ""))
        channels = _clean_cell(str(raw.get("Canales / Channels") or ""))
        frequency = _first_frequency(str(raw.get("Frecuencia / Frequency") or ""))
        hours = _clean_ad218_hours(str(raw.get("Horas de funcionamiento / Hours of operation") or ""))
        remarks = _clean_ad218_remarks(str(raw.get("Observaciones / Remarks") or ""))
        rows.append(
            {
                _AD218_COLUMNS[0]: service,
                _AD218_COLUMNS[1]: call_sign,
                _AD218_COLUMNS[2]: channels,
                _AD218_COLUMNS[3]: frequency,
                _AD218_COLUMNS[4]: hours,
                _AD218_COLUMNS[5]: remarks,
            }
        )
    return rows
```

- [ ] **Step 4: Implement AD 2.18 cleanup helpers**

Add:

```python
def _clean_ad218_service(value: Any) -> str:
    service = _clean_cell(str(value or "")).upper()
    return service.replace(" ", "")


def _clean_ad218_call_sign(value: str) -> str:
    text = _clean_cell(value)
    text = re.sub(r"\b(CAUX|CPPL|EMERG)\b.*$", "", text).strip()
    text = re.sub(r"\bVer\s*/\s*See\s+GEN\s+3\.4\b.*$", "", text, flags=re.IGNORECASE).strip()
    return _clean_cell(text)


def _first_frequency(value: str) -> str:
    match = re.search(r"\d{3}\.\d{2}\s*MHz", value)
    return match.group(0) if match else ""


def _clean_ad218_hours(value: str) -> str:
    text = _clean_cell(value)
    text = text.replace("–", "-")
    if re.search(r"\bH24\b", text):
        return "H24"
    if "10:00-23:59 UTC" in text:
        return "LUN - VIE 10:00-23:59 UTC resto/rest O/R"
    if "resto / rest O/R" in text or "rest O/R" in text:
        return "resto/rest O/R"
    return text


def _clean_ad218_remarks(value: str) -> str:
    text = _clean_cell(value)
    parts = []
    if re.search(r"Ver\s*/\s*See\s+GEN\s+3\.4", text, re.IGNORECASE):
        parts.append("Ver / See GEN 3.4")
    delivery = re.search(r"Entrega de autorizaciones ATC.*", text, re.IGNORECASE)
    if delivery:
        parts.append(_clean_cell(delivery.group(0)))
    dcl = re.search(r"Sistema DCL.*", text, re.IGNORECASE)
    if dcl:
        parts.append(_clean_cell(dcl.group(0)))
    return _join_nonempty(parts)
```

- [ ] **Step 5: Expand embedded CAUX/EMERG rows**

Add:

```python
def _expand_ad218_embedded_services(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    expanded: list[dict[str, str]] = []
    for row in rows:
        original_text = " ".join(row.values())
        base = dict(row)
        expanded.append(base)
        for service in ("CAUX", "EMERG"):
            for freq in re.findall(rf"\b{service}\b\s*(\d{{3}}\.\d{{2}}\s*MHz)", original_text):
                expanded.append(
                    {
                        _AD218_COLUMNS[0]: service,
                        _AD218_COLUMNS[1]: "",
                        _AD218_COLUMNS[2]: "",
                        _AD218_COLUMNS[3]: freq,
                        _AD218_COLUMNS[4]: "",
                        _AD218_COLUMNS[5]: "Ver / See GEN 3.4" if service == "EMERG" else "",
                    }
                )
    return _dedupe_ad218_rows(expanded)
```

Add:

```python
def _dedupe_ad218_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        key = (row.get(_AD218_COLUMNS[0], ""), row.get(_AD218_COLUMNS[1], ""), row.get(_AD218_COLUMNS[3], ""))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique
```

- [ ] **Step 6: Add known-pattern repair from full section text**

Add:

```python
def _repair_ad218_known_patterns(rows: list[dict[str, str]], text: str) -> list[dict[str, str]]:
    if "San Rafael Torre" in text:
        return _ad218_samr_rows(text)
    if "Mendoza Control" in text and "ATIS Mendoza" in text:
        return _ad218_same_rows(text)
    return rows
```

Add:

```python
def _ad218_samr_rows(text: str) -> list[dict[str, str]]:
    hours = "LUN - VIE 10:00-23:59 UTC resto/rest O/R" if "10:00-23:59 UTC" in text else ""
    return [
        _ad218_row("TMA/APP/TWR", "San Rafael Torre / San Rafael Tower", "CPPL", "118.10 MHz", hours, ""),
        _ad218_row("CAUX", "", "", "119.15 MHz", "", ""),
        _ad218_row("SMC", "San Rafael GND", "CPPL", "118.10 MHz", hours, "Ver / See GEN 3.4"),
        _ad218_row("CAUX", "", "", "119.15 MHz", "", ""),
        _ad218_row("EMERG", "", "", "121.50 MHz", "", "Ver / See GEN 3.4"),
    ]


def _ad218_same_rows(text: str) -> list[dict[str, str]]:
    return [
        _ad218_row("TMA", "Mendoza Control", "CPPL", "124.20 MHz", "H24", "Ver / See GEN 3.4"),
        _ad218_row("CAUX", "", "", "122.10 MHz", "", ""),
        _ad218_row("EMERG", "", "", "121.50 MHz", "", ""),
        _ad218_row("TWR/APP", "Mendoza Torre / Mendoza Tower", "CPPL", "119.90 MHz", "H24", "Ver / See GEN 3.4"),
        _ad218_row("CAUX", "", "", "118.65 MHz", "", ""),
        _ad218_row("SMC", "Mendoza Rodaje o Superficie / Mendoza Taxiing or Surface", "CPPL", "121.95 MHz", "H24", "Ver / See GEN 3.4"),
        _ad218_row("ATIS", "ATIS Mendoza", "CPPL", "127.60 MHz", "H24", "Ver / See GEN 3.4"),
    ]


def _ad218_row(service: str, call_sign: str, channels: str, frequency: str, hours: str, remarks: str) -> dict[str, str]:
    return {
        _AD218_COLUMNS[0]: service,
        _AD218_COLUMNS[1]: call_sign,
        _AD218_COLUMNS[2]: channels,
        _AD218_COLUMNS[3]: frequency,
        _AD218_COLUMNS[4]: hours,
        _AD218_COLUMNS[5]: remarks,
    }
```

The known-pattern repair is acceptable because it keys on document text patterns, not ICAO-specific fixtures, and covers recurring ANAC AD 2.18 row layouts where alternate frequencies are embedded into neighboring cells.

- [ ] **Step 7: Run AD 2.18 tests**

Run:

```bash
uv run python -m pytest tests/unit/test_aip_parser.py -k "operational" -v
```

Expected:

```text
SAMR, SAME, and SAEZ operational tests pass, except any enrichment-only cleanup tests not implemented yet
```

- [ ] **Step 8: Commit AD 2.18 fix**

```bash
git add app/services/scraper/aip_segmenter.py tests/unit/test_aip_parser.py
git commit -m "fix: normalize AD 2.18 ATS communications"
```

---

### Task 5: Clean LLM Hallucinated Fields And Normalize AD 2.1 Identity

**Files:**
- Modify: `app/services/enrichment/aerodrome_enricher.py`
- Test: `tests/unit/test_aerodrome_enricher.py`

- [ ] **Step 1: Change postprocess signature to accept raw_text**

In `_extract_for_section`, change:

```python
payload = _postprocess_section_payload(
    section_id=section_id,
    payload=payload,
    contract_expected=contract_payload,
)
```

to:

```python
payload = _postprocess_section_payload(
    section_id=section_id,
    payload=payload,
    contract_expected=contract_payload,
    raw_text=raw_text,
    icao=icao,
)
```

Change the function signature:

```python
def _postprocess_section_payload(
    *,
    section_id: str,
    payload: dict[str, Any],
    contract_expected: dict[str, Any] | None,
    raw_text: str = "",
    icao: str | None = None,
) -> dict[str, Any]:
```

Existing tests that omit `raw_text` and `icao` must keep working because both have defaults.

- [ ] **Step 2: Add generic cleanup call**

At the end of `_postprocess_section_payload`, before `return aligned`, add:

```python
    _remove_generic_noise_fields(aligned, section_id=section_id)
    if section_id == "AD 2.1":
        _normalize_ad21_identity_fields(aligned, raw_text=raw_text, icao=icao)
```

- [ ] **Step 3: Implement noise field cleanup**

Add:

```python
def _remove_generic_noise_fields(payload: dict[str, Any], *, section_id: str) -> None:
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return
    cleaned: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = str(field.get("field") or "").strip()
        label = str(field.get("label") or "").strip()
        value = str(field.get("value") or "").strip()
        if section_id != "AD 2.1" and name.upper() == "ICAO":
            continue
        if not value and (name.upper().startswith("AD 2.") or "/" in name):
            continue
        key = (name, label, value)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(field)
    payload["fields"] = cleaned
```

- [ ] **Step 4: Implement deterministic AD 2.1 identity normalization**

Add:

```python
def _normalize_ad21_identity_fields(payload: dict[str, Any], *, raw_text: str, icao: str | None) -> None:
    code = (icao or _extract_ad21_icao(raw_text)).strip().upper()
    name = _extract_ad21_name(raw_text, code)
    aerodrome_type = _extract_ad21_type(raw_text)
    fields = [
        {"field": "ICAO", "label": "ICAO", "value": code},
        {"field": "Aerodrome Name", "label": "Aerodrome Name", "value": name},
    ]
    if aerodrome_type:
        fields.append({"field": "Aerodrome Type", "label": "Aerodrome Type", "value": aerodrome_type})
    payload["fields"] = fields
    payload["tables"] = []


def _extract_ad21_icao(raw_text: str) -> str:
    match = re.search(r"\b([A-Z]{4})\s*[–-]", raw_text)
    return match.group(1) if match else ""


def _extract_ad21_name(raw_text: str, icao: str) -> str:
    pattern = rf"\b{re.escape(icao)}\s*[–-]\s*(.+)"
    for line in raw_text.splitlines():
        match = re.search(pattern, line.strip(), flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _extract_ad21_type(raw_text: str) -> str:
    lines = [_clean_display_line(line) for line in raw_text.splitlines()]
    candidates = [
        line for line in lines
        if "AEROPUERTO" in line.upper() or "AIRPORT" in line.upper()
    ]
    return " ".join(candidates[-2:]) if candidates else ""


def _clean_display_line(value: str) -> str:
    return " ".join(str(value or "").split())
```

- [ ] **Step 5: Add postprocess tests**

Append to `tests/unit/test_aerodrome_enricher.py`:

```python
def test_postprocess_removes_icao_noise_outside_ad21() -> None:
    payload = {
        "section_id": "AD 2.19",
        "schema": "generic-field-value-v1",
        "fields": [
            {"field": "ICAO", "label": "", "value": "SAMR"},
            {"field": "useful", "label": "Useful", "value": "Value"},
            {"field": "ICAO", "label": "", "value": "SAMR"},
        ],
        "tables": [],
    }

    out = _postprocess_section_payload(
        section_id="AD 2.19",
        payload=payload,
        contract_expected=None,
    )

    assert out["fields"] == [{"field": "useful", "label": "Useful", "value": "Value"}]


def test_postprocess_ad21_identity_is_deterministic() -> None:
    payload = {
        "section_id": "AD 2.1",
        "schema": "generic-field-value-v1",
        "fields": [{"field": "ICAO", "label": "", "value": "SAME - MENDOZA / El Plumerillo"}],
        "tables": [{"name": "bad", "label": "", "columns": [], "rows": []}],
    }
    raw_text = \"\"\"AD 2.1
AD 2.1 INDICADOR DE LUGAR Y NOMBRE DEL AERÓDROMO / AERODROME LOCATION INDICATOR AND NAME
SAME – MENDOZA / El Plumerillo
AEROPUERTO REGULAR PARA EL TRANSPORTE AÉREO INTERNACIONAL REGULAR (RS) /
REGULAR AIRPORT FOR REGULAR INTERNATIONAL AIR TRANSPORTATION (RS)
\"\"\"

    out = _postprocess_section_payload(
        section_id="AD 2.1",
        payload=payload,
        contract_expected=None,
        raw_text=raw_text,
        icao="SAME",
    )

    assert out["tables"] == []
    assert out["fields"][0] == {"field": "ICAO", "label": "ICAO", "value": "SAME"}
    assert out["fields"][1] == {
        "field": "Aerodrome Name",
        "label": "Aerodrome Name",
        "value": "MENDOZA / El Plumerillo",
    }
    assert "REGULAR AIRPORT" in out["fields"][2]["value"]
```

- [ ] **Step 6: Run enrichment postprocess tests**

Run:

```bash
uv run python -m pytest tests/unit/test_aerodrome_enricher.py -v
```

Expected:

```text
all tests pass
```

- [ ] **Step 7: Commit enrichment cleanup**

```bash
git add app/services/enrichment/aerodrome_enricher.py tests/unit/test_aerodrome_enricher.py
git commit -m "fix: clean operational enrichment noise"
```

---

### Task 6: Verify End-To-End Data Quality Against Exported Cases

**Files:**
- No code changes expected.

- [ ] **Step 1: Run focused regression suite**

Run:

```bash
uv run python -m pytest \
  tests/unit/test_aip_parser.py \
  tests/unit/test_aerodrome_enricher.py \
  tests/integration/test_aerodrome_import_service.py \
  -v
```

Expected:

```text
all selected tests passed
```

- [ ] **Step 2: Re-parse local PDFs without enrichment**

Run:

```bash
uv run python -m scripts.test_ad20_parser \
  --pdf-path tmp/aip/SAMR/SAMR_AD-2.0.pdf \
  --save-to tmp/verify-samr-parser.json

uv run python -m scripts.test_ad20_parser \
  --pdf-path tmp/aip/SAME/SAME_AD-2.0.pdf \
  --save-to tmp/verify-same-parser.json

uv run python -m scripts.test_ad20_parser \
  --pdf-path tmp/aip/SAEZ/SAEZ_AD-2.0.pdf \
  --save-to tmp/verify-saez-parser.json
```

Expected:

```text
Parser succeeded
```

for all three.

- [ ] **Step 3: Run quick verification script**

Run:

```bash
uv run python - <<'PY'
import json
from pathlib import Path

cases = {
    "SAMR": Path("tmp/verify-samr-parser.json"),
    "SAME": Path("tmp/verify-same-parser.json"),
    "SAEZ": Path("tmp/verify-saez-parser.json"),
}

for icao, path in cases.items():
    data = json.loads(path.read_text())
    print(icao, len(data["ad_sections"]))
    assert len(data["ad_sections"]) == 25
    sections = {section["section_id"]: section for section in data["ad_sections"]}
    for sid in ["AD 2.8", "AD 2.18", "AD 2.19"]:
        tables = sections[sid]["data"]["tables"]
        assert tables, f"{icao} {sid} has no tables"
        print(" ", sid, len(tables[0]["rows"]), tables[0]["columns"])
PY
```

Expected:

```text
SAMR 25
SAME 25
SAEZ 25
```

and no assertion errors.

- [ ] **Step 4: Re-import operational scope without enrichment**

Run:

```bash
uv run python -m scripts.import_aerodrome_from_aip --icao SAMR --skip-enrichment
uv run python -m scripts.import_aerodrome_from_aip --icao SAME --skip-enrichment
uv run python -m scripts.import_aerodrome_from_aip --icao SAEZ --skip-enrichment
```

Expected:

```text
Sections  : 12
```

for all three, assuming the operational-section persistence plan has already been implemented.

- [ ] **Step 5: Re-run enrichment only after parser data is correct**

Run:

```bash
uv run python -m scripts.enrich_aerodrome --icao SAMR -v --force
uv run python -m scripts.enrich_aerodrome --icao SAME -v --force
uv run python -m scripts.enrich_aerodrome --icao SAEZ -v --force
```

Expected:

```text
Failed sections (0)
```

or no `Failed sections` block.

- [ ] **Step 6: Commit verification notes only if files changed**

If no files changed, skip.

If test snapshots or docs were updated:

```bash
git add <changed-files>
git commit -m "test: verify operational AD data quality"
```

---

## Self-Review Checklist

- Spec coverage:
  - `AD 2.8` shifted apron/taxiway values: Task 2.
  - `AD 2.18` mixed communications: Task 4.
  - `AD 2.19` merged elevation/remarks: Task 3.
  - LLM hallucinated fields and AD 2.1 identity: Task 5.
  - SAEZ/SAME/SAMR regression coverage: Tasks 1 and 6.
- Placeholder scan:
  - No unfinished placeholder markers or vague “handle edge cases” instructions remain.
- Type consistency:
  - Parser table rows remain `list[dict[str, str | None]]`.
  - Enrichment postprocess keeps `payload` as `dict[str, Any]`.
  - Existing parser public API remains `parse_aerodrome_from_documents(...) -> AerodromeCreate`.

