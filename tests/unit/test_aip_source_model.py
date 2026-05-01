from app.services.scraper.aip_source_model import (
    SourceRow,
    build_section_source,
    merge_continuation_rows,
    normalize_bilingual_pairs,
    normalize_bullet_text,
)


def test_build_section_source_detects_table_and_list_rows() -> None:
    raw = """AD 2.18 ATS COMMUNICATION FACILITIES
| TMA/APP/TWR | San Rafael Torre | 118.10 MHz |
- First bullet
Plain text
"""

    model = build_section_source("AD 2.18", raw)

    assert model.section_id == "AD 2.18"
    assert any(row.kind == "table" for row in model.rows)
    assert any(row.kind == "bullet" for row in model.rows)
    assert any(row.kind == "text" for row in model.rows)


def test_merge_continuation_rows_appends_following_text_line() -> None:
    rows = [
        SourceRow(kind="table", raw="| 11 | 108.46 |", cells=["11", "108.46"]),
        SourceRow(kind="text", raw="343510.2S 0682717.9W", cells=[]),
    ]

    merged = merge_continuation_rows(rows)

    assert len(merged) == 1
    assert merged[0].cells == ["11", "108.46 343510.2S 0682717.9W"]


def test_normalize_bilingual_pairs_compacts_slash_spacing() -> None:
    assert normalize_bilingual_pairs("Servicio /  Service") == "Servicio / Service"
    assert normalize_bilingual_pairs("Servicio/ Service") == "Servicio / Service"


def test_normalize_bullet_text_removes_marker() -> None:
    assert normalize_bullet_text("- Texto de prueba") == "Texto de prueba"
    assert normalize_bullet_text("* Texto de prueba") == "Texto de prueba"
