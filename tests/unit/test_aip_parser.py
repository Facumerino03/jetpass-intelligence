"""Unit tests for PyMuPDF AD 2.0 parsing."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.schemas.aerodrome import AerodromeCreate
from app.services.scraper.aip_parser import (
    PdfFormatError,
    PdfNotReadableError,
    PyMuPdfAipParser,
    parse_aerodrome_from_ad20,
    parse_aerodrome_from_documents,
)
from app.services.scraper.aip_segmenter import sectionize_layout_artifact


def _layout_artifact(*, missing: str | None = None) -> dict:
    elements = []
    order = 1
    elements.append(
        {
            "type": "text",
            "text": "AD 2.1 INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / AERODROME LOCATION INDICATOR AND NAME\nSAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO",
            "page": 1,
            "bbox": [80, 70, 500, 120],
            "order": order,
        }
    )
    order += 1
    for idx in range(2, 26):
        sid = f"AD 2.{idx}"
        if sid == missing:
            continue
        elements.append(
            {
                "type": "table",
                "text": f"{sid} SECTION {idx}\n1 | Label {idx} | Value {idx}",
                "page": 1 + idx // 4,
                "bbox": [42, 140 + idx, 550, 180 + idx],
                "order": order,
                "table": {
                    "label": f"{sid} SECTION {idx}",
                    "columns": ["item", "label", "value"],
                    "rows": [{"item": "1", "label": f"Label {idx}", "value": f"Value {idx}"}],
                    "cells": ["1", f"Label {idx}", f"Value {idx}"],
                    "raw_rows": [[sid, f"SECTION {idx}"], ["1", f"Label {idx}", f"Value {idx}"]],
                },
            }
        )
        order += 1
    return {
        "schema_version": "aip-layout-v1",
        "engine": "pymupdf",
        "pages": [{"page": 1, "width": 595, "height": 842, "elements": elements}],
    }


def test_sectionize_layout_returns_25_sections_with_tables() -> None:
    result = sectionize_layout_artifact(
        layout_artifact=_layout_artifact(),
        icao="SAMR",
        source_path=Path("SAMR_AD-2.0.pdf"),
        logger=object(),
        format_error=PdfFormatError,
    )

    assert len(result.sections) == 25
    assert result.sections[0].section_id == "AD 2.1"
    assert result.sections[0].raw_text.startswith("AD 2.1")
    ad22 = next(section for section in result.sections if section.section_id == "AD 2.2")
    assert ad22.data["tables"][0]["rows"][0]["value"] == "Value 2"
    assert ad22.anchors["section_blocks"][0]["bbox"] == [42, 142, 550, 182]
    assert ad22.section_title == "SECTION 2"


def test_sectionize_layout_raises_with_missing_section_diagnostics() -> None:
    with pytest.raises(PdfFormatError, match=r"missing=\['AD 2.12'\]"):
        sectionize_layout_artifact(
            layout_artifact=_layout_artifact(missing="AD 2.12"),
            icao="SAMR",
            source_path=Path("SAMR_AD-2.0.pdf"),
            logger=object(),
            format_error=PdfFormatError,
        )


def test_section_title_does_not_include_table_rows() -> None:
    artifact = _layout_artifact()
    table = artifact["pages"][0]["elements"][1]
    table["text"] = "AD 2.2 TITLE / ENGLISH TITLE\n1 | Label | Value"
    table["table"]["label"] = "AD 2.2 TITLE /"
    table["table"]["raw_rows"] = [["AD 2.2 TITLE /"], ["1", "Label", "Value"]]

    result = sectionize_layout_artifact(
        layout_artifact=artifact,
        icao="SAMR",
        source_path=Path("SAMR_AD-2.0.pdf"),
        logger=object(),
        format_error=PdfFormatError,
    )

    ad22 = next(section for section in result.sections if section.section_id == "AD 2.2")
    assert ad22.section_title == "TITLE / ENGLISH TITLE"
    assert "Label" not in ad22.section_title


def test_pymupdf_parser_rejects_empty_pdf(tmp_path: Path) -> None:
    pdf_path = tmp_path / "empty.pdf"
    pdf_path.touch()

    with pytest.raises(PdfNotReadableError, match="empty"):
        PyMuPdfAipParser().extract_layout(pdf_path)


def test_parse_aerodrome_from_ad20_infers_icao_from_filename(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pdf_path = tmp_path / "SAMR_AD-2.0.pdf"
    pdf_path.write_bytes(b"%PDF-1.7\n")

    def fake_parse(paths: list[Path], *, icao: str) -> AerodromeCreate:
        return AerodromeCreate(
            icao_code=icao,
            name="San Rafael",
            ad_sections=[
                {
                    "section_id": f"AD 2.{idx}",
                    "title": f"AD 2.{idx}",
                    "raw_text": f"AD 2.{idx}",
                }
                for idx in range(1, 26)
            ],
        )

    monkeypatch.setattr("app.services.scraper.aip_parser.parse_aerodrome_from_documents", fake_parse)

    result = parse_aerodrome_from_ad20(pdf_path)

    assert result.icao_code == "SAMR"


def test_parse_real_samr_pdf_when_available() -> None:
    pdf_path = Path("/mnt/c/Users/facun/Downloads/SAMR PLAN DE VUELO/AIP-SAMR.pdf")
    if not pdf_path.exists():
        pytest.skip("Local SAMR AIP fixture is not available.")

    result = parse_aerodrome_from_documents([pdf_path], icao="SAMR")

    assert result.name == "San Rafael"
    assert len(result.ad_sections) == 25
    ad22 = next(section for section in result.ad_sections if section.section_id == "AD 2.2")
    assert ad22.data["tables"]
    assert ad22.anchors and ad22.anchors["section_blocks"][0]["bbox"]


def test_parse_real_saez_ad212_declared_tables_when_available() -> None:
    pdf_path = Path("/mnt/c/Users/facun/Downloads/SAEZ AIP.pdf")
    if not pdf_path.exists():
        pytest.skip("Local SAEZ AIP fixture is not available.")

    result = parse_aerodrome_from_documents([pdf_path], icao="SAEZ")
    ad212 = next(section for section in result.ad_sections if section.section_id == "AD 2.12")

    assert ad212.section_title == "CARACTERÍSTICAS FÍSICAS DE LAS PISTAS / RUNWAY PHYSICAL CHARACTERISTICS"
    assert len(ad212.data["tables"]) == 2
    first = ad212.data["tables"][0]
    second = ad212.data["tables"][1]
    row_35 = next(row for row in first["rows"] if row["RWY"] == "35")
    assert row_35["Dimensions of RWY (m)"] == "3.105x45"
    assert row_35["THR coordinates"] == "344957.32S 0583131.79W"
    assert row_35["Slope RWY-SWY"] == "-0.01%"
    supp_35 = next(row for row in second["rows"] if row["RWY"] == "35")
    assert supp_35["CWY (m)"] == "300x150"
    assert "DTHR 35 300 m" in supp_35["Remarks"]


def test_parse_real_saez_normalizes_lighting_comms_and_charts_when_available() -> None:
    pdf_path = Path("/mnt/c/Users/facun/Downloads/SAEZ AIP.pdf")
    if not pdf_path.exists():
        pytest.skip("Local SAEZ AIP fixture is not available.")

    result = parse_aerodrome_from_documents([pdf_path], icao="SAEZ")

    ad214 = next(section for section in result.ad_sections if section.section_id == "AD 2.14")
    assert len(ad214.data["tables"]) == 2
    lighting_29 = next(
        row for row in ad214.data["tables"][0]["rows"]
        if row["Designador RWY / RWY designator"] == "29"
    )
    assert lighting_29["LGT THR Color WBAR / THR LGT Color WBAR"] == "Sí/Yes"
    assert lighting_29["PAPI, VASIS"] == "Ángulo de aproximación 2.95° / Approach angle 2,95°"
    assert lighting_29["LEN LGT TDZ"] == "No"
    rcll_35 = next(
        row for row in ad214.data["tables"][1]["rows"]
        if row["Designador RWY / RWY designator"] == "35"
    )
    assert rcll_35["Observaciones / Remarks"] == "NIL"

    ad218 = next(section for section in result.ad_sections if section.section_id == "AD 2.18")
    comm_rows = ad218.data["tables"][0]["rows"]
    assert "Horas de funcionamiento / Hours of operation" in ad218.data["tables"][0]["columns"]
    twr = next(row for row in comm_rows if row["Designacion del Servicio / Service designation"] == "TWR")
    assert twr["Canales / Channels"] == "CPPL"
    assert twr["Horas de funcionamiento / Hours of operation"] == "H24"
    atis = next(row for row in comm_rows if row["Designacion del Servicio / Service designation"] == "ATIS")
    assert atis["Distintivo de llamada / Call sign"] == "ATIS Ezeiza / Ezeiza ATIS"
    assert atis["Observaciones / Remarks"] == "Ver/See GEN 3.4."
    clrd_dcl = [
        row for row in comm_rows
        if row["Designacion del Servicio / Service designation"] == "CLRD"
        and "DCL system" in row["Observaciones / Remarks"]
    ][0]
    assert clrd_dcl["Horas de funcionamiento / Hours of operation"] == "H24"

    ad224 = next(section for section in result.ad_sections if section.section_id == "AD 2.24")
    chart_rows = ad224.data["tables"][0]["rows"]
    commercial_apron = next(row for row in chart_rows if "Commercial Apron" in row["Carta / Chart"])
    assert commercial_apron["Codigo / Code"] == "SAEZ AD 2.B1-B2-B3-B4-B5-B6-B7-B8-B9"
    assert "SAEZ AD" not in commercial_apron["Carta / Chart"]
    sid = next(row for row in chart_rows if row["Carta / Chart"] == "ATOVO 3A-BIVAM 3A-LANDA 3A")
    assert "salida normalizada" in sid["Categoria / Category"]
    assert sid["Pista / RWY"] == "Pista/RWY 11"
