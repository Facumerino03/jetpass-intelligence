"""Unit tests for field-by-field golden evaluator."""

from __future__ import annotations

from tests.unit.golden.contracts import FieldRule
from tests.unit.golden.contracts import RulesFile
from tests.unit.golden.evaluator import evaluate_field_rule, evaluate_section


def test_evaluate_text_with_normalization_passes() -> None:
    actual_data = {"identity": {"display_name": "samr - san rafael"}}
    rule = FieldRule(
        field="identity.display_name",
        type="text",
        criticality="high",
        expected="SAMR – SAN RAFAEL",
        normalization=["strip", "collapse_spaces", "uppercase", "normalize_dash"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="SAMR - SAN RAFAEL")

    assert result.status == "pass"
    assert result.probable_cause == "ok"


def test_evaluate_number_with_tolerance_passes() -> None:
    actual_data = {"geographical": {"elevation_ft": 2475.6}}
    rule = FieldRule(
        field="geographical.elevation_ft",
        type="number",
        criticality="high",
        expected=2476,
        tolerance=1.0,
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="ELEV 2476 FT")

    assert result.status == "pass"
    assert result.probable_cause == "ok"


def test_evaluate_reports_missing_actual_field() -> None:
    actual_data = {"identity": {}}
    rule = FieldRule(
        field="identity.location_indicator",
        type="text",
        criticality="high",
        expected="SAMR",
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "fail"
    assert result.probable_cause == "actual_missing"


def test_evaluate_reports_numeric_tolerance_exceeded() -> None:
    actual_data = {"geographical": {"elevation_ft": 2480}}
    rule = FieldRule(
        field="geographical.elevation_ft",
        type="number",
        criticality="high",
        expected=2476,
        tolerance=1.0,
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="ELEV 2476 FT")

    assert result.status == "fail"
    assert result.probable_cause == "numeric_tolerance_exceeded"


def test_evaluate_reports_parser_input_quality_when_evidence_missing() -> None:
    actual_data = {"identity": {"name": "WRONG NAME"}}
    rule = FieldRule(
        field="identity.name",
        type="text",
        criticality="high",
        expected="SAN RAFAEL",
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="DOCUMENT WITHOUT EXPECTED TOKEN")

    assert result.status == "fail"
    assert result.probable_cause == "raw_missing"


def test_evaluate_reports_prompt_extraction_gap_when_evidence_present() -> None:
    actual_data = {"identity": {"name": "WRONG NAME"}}
    rule = FieldRule(
        field="identity.name",
        type="text",
        criticality="high",
        expected="SAN RAFAEL",
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="... SAN RAFAEL ...")

    assert result.status == "fail"
    assert result.probable_cause == "normalization_gap"


def test_evaluate_detects_evidence_despite_missing_diacritics() -> None:
    actual_data = {"administrative": {"ad_head_office": {"name_es": "ADMINISTRACION NACIONAL"}}}
    rule = FieldRule(
        field="administrative.ad_head_office.name_es",
        type="text",
        criticality="high",
        expected="ADMINISTRACIÓN NACIONAL",
    )

    result = evaluate_field_rule(
        rule=rule,
        actual_data=actual_data,
        raw_text="ADMINISTRACION NACIONAL DE AVIACION CIVIL",
    )

    assert result.status == "pass"


def test_evaluate_object_presence_passes_when_field_exists() -> None:
    actual_data = {"runways": {"runways": []}}
    rule = FieldRule(
        field="runways",
        type="object_presence",
        criticality="high",
        expected=True,
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"
    assert result.probable_cause == "ok"


def test_evaluate_uses_expected_data_when_rule_expected_is_null() -> None:
    actual_data = {"identity": {"location_indicator": "SAMR"}}
    expected_data = {"identity": {"location_indicator": "SAMR"}}
    rule = FieldRule(
        field="identity.location_indicator",
        type="text",
        criticality="high",
        expected=None,
    )

    result = evaluate_field_rule(
        rule=rule,
        actual_data=actual_data,
        raw_text="SAMR",
        expected_data=expected_data,
    )

    assert result.status == "pass"
    assert result.expected == "SAMR"


def test_normalize_units_does_not_break_domains_or_emails() -> None:
    actual_data = {"contact": {"website": "www.aeropuertosargentina.com"}}
    rule = FieldRule(
        field="contact.website",
        type="text",
        criticality="high",
        expected="www.aeropuertosargentina.com",
        normalization=["normalize_units"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"
    assert result.normalized_expected == "www.aeropuertosargentina.com"


def test_normalize_slash_spacing_treats_bilingual_spacing_as_equal() -> None:
    actual_data = {"row": {"label": "PISTA/ RWY 29"}}
    rule = FieldRule(
        field="row.label",
        type="text",
        criticality="high",
        expected="PISTA / RWY 29",
        normalization=["normalize_slash_spacing"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_normalize_bullets_treats_leading_list_markers_as_equal() -> None:
    actual_data = {"notes": {"vfr": "Las OPS VFR deberan ajustarse"}}
    rule = FieldRule(
        field="notes.vfr",
        type="text",
        criticality="high",
        expected="- Las OPS VFR deberan ajustarse",
        normalization=["normalize_bullets", "collapse_spaces"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_normalize_table_punctuation_handles_pipe_spacing_and_dashes() -> None:
    actual_data = {"table": {"row": "A | B | C - NIL"}}
    rule = FieldRule(
        field="table.row",
        type="text",
        criticality="medium",
        expected="A|B|C – NIL",
        normalization=["normalize_table_punctuation"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_evaluate_section_table_only_compares_cells() -> None:
    rules = RulesFile(section_id="AD 2.13", version="3.0", fields=[])
    expected_data = {
        "section_id": "AD 2.13",
        "schema": "generic-field-value-v1",
        "fields": [],
        "tables": [
            {
                "name": "table_1",
                "label": "Declared distances",
                "columns": ["Designador", "TORA"],
                "rows": [{"Designador": "11", "TORA": "2.102"}],
            }
        ],
    }
    actual_data = {
        "section_id": "AD 2.13",
        "schema": "generic-field-value-v1",
        "fields": [],
        "tables": [
            {
                "name": "table_1",
                "label": "Declared distances",
                "columns": ["Designador", "TORA"],
                "rows": [{"Designador": "11", "TORA": "2.102"}],
            }
        ],
    }

    report = evaluate_section(
        icao="SAMR",
        rules=rules,
        actual_data=actual_data,
        raw_text="11 2.102",
        expected_data=expected_data,
    )

    assert report.summary["high_failed"] == 0
    assert report.summary["passed"] == 2


def test_item_field_falls_back_to_label_match_when_field_id_differs() -> None:
    actual_data = {
        "fields": [
            {
                "field": "AerodromeDisplayName",
                "label": "INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / AERODROME LOCATION INDICATOR AND NAME",
                "value": "SAMR  SAN RAFAEL / S. A. SANTIAGO GERMANO",
            }
        ]
    }
    rule = FieldRule(
        field="item_1",
        label="INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / AERODROME LOCATION INDICATOR AND NAME",
        type="text",
        criticality="high",
        expected="SAMR  SAN RAFAEL / S. A. SANTIAGO GERMANO",
        normalization=["strip", "collapse_spaces"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_item_field_falls_back_to_expected_value_match_when_label_differs() -> None:
    actual_data = {
        "fields": [
            {
                "field": "AerodromeDisplayName",
                "label": "Aerodrome Display Name",
                "value": "SAMR - SAN RAFAEL / S. A. SANTIAGO GERMANO",
            }
        ]
    }
    rule = FieldRule(
        field="item_1",
        label="INDICADOR DE LUGAR Y NOMBRE DEL AERODROMO / AERODROME LOCATION INDICATOR AND NAME",
        type="text",
        criticality="high",
        expected="SAMR  SAN RAFAEL / S. A. SANTIAGO GERMANO",
        normalization=["strip", "collapse_spaces", "normalize_dash", "normalize_quotes"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_evaluate_text_passes_with_diacritics_dash_quote_variants() -> None:
    actual_data = {"identity": {"name": "ADMINISTRACION NACIONAL - 1° W / 11' W"}}
    rule = FieldRule(
        field="identity.name",
        type="text",
        criticality="high",
        expected="ADMINISTRACIÓN NACIONAL – 1° W / 11’ W",
        normalization=["strip", "collapse_spaces", "normalize_dash", "normalize_quotes"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"


def test_evaluate_text_passes_with_minor_orthographic_variants() -> None:
    actual_data = {
        "x": {
            "value": "Empresa Argentina de Navegación Aérea (EANA SA), ARO-AIS (+54 260) 4430703"
        }
    }
    rule = FieldRule(
        field="x.value",
        type="text",
        criticality="high",
        expected="Empresa Argentina de Navegacion Area (EANA SA), ARO-AIS (+54 260) 4430703",
        normalization=["strip", "collapse_spaces"],
    )

    result = evaluate_field_rule(rule=rule, actual_data=actual_data, raw_text="")

    assert result.status == "pass"
