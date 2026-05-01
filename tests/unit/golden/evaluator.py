"""Field-level evaluator for golden AD 2.x tests."""

from __future__ import annotations

from difflib import SequenceMatcher
import re
import unicodedata
from typing import Any

from tests.unit.golden.contracts import FieldResult, FieldRule, RulesFile, SectionReport


_MISSING = object()


def evaluate_field_rule(
    *,
    rule: FieldRule,
    actual_data: dict[str, Any],
    raw_text: str,
    expected_data: dict[str, Any] | None = None,
) -> FieldResult:
    expected_value = rule.expected
    if expected_value is None and expected_data is not None:
        resolved_expected = _resolve_path(expected_data, rule.field)
        if resolved_expected is not _MISSING:
            expected_value = resolved_expected

    actual = _resolve_path(actual_data, rule.field)
    if actual is _MISSING and re.fullmatch(r"item_\d+", rule.field) and rule.label:
        by_label = _resolve_field_value_by_label(actual_data, rule.label)
        if by_label is not _MISSING:
            actual = by_label
    if actual is _MISSING and re.fullmatch(r"item_\d+", rule.field):
        by_value = _resolve_field_value_by_expected(actual_data, expected_value)
        if by_value is not _MISSING:
            actual = by_value
    if actual is _MISSING:
        return FieldResult(
            field=rule.field,
            status="fail",
            expected=expected_value,
            actual=None,
            normalized_expected=_normalize(expected_value, rule.normalization),
            normalized_actual=None,
            type=rule.type,
            criticality=rule.criticality,
            probable_cause="actual_missing",
            evidence_in_raw_text=False,
            message=f"Field not found at path '{rule.field}'.",
        )

    normalized_expected = _normalize(expected_value, rule.normalization)
    normalized_actual = _normalize(actual, rule.normalization)

    passed, compare_cause = _compare(rule, normalized_expected, normalized_actual)
    evidence = _has_evidence(expected_value, raw_text)
    probable_cause = "ok" if passed else _derive_probable_cause(compare_cause, evidence)

    return FieldResult(
        field=rule.field,
        status="pass" if passed else "fail",
        expected=expected_value,
        actual=actual,
        normalized_expected=normalized_expected,
        normalized_actual=normalized_actual,
        type=rule.type,
        criticality=rule.criticality,
        probable_cause=probable_cause,
        evidence_in_raw_text=evidence,
        message=(
            "Values match."
            if passed
            else f"Expected {expected_value!r} but got {actual!r}."
        ),
    )


def evaluate_section(
    *,
    icao: str,
    rules: RulesFile,
    actual_data: dict[str, Any],
    raw_text: str,
    expected_data: dict[str, Any] | None = None,
) -> SectionReport:
    if rules.fields:
        results = [
            evaluate_field_rule(
                rule=rule,
                actual_data=actual_data,
                raw_text=raw_text,
                expected_data=expected_data,
            )
            for rule in rules.fields
        ]
    else:
        results = _evaluate_table_only_section(
            actual_data=actual_data,
            raw_text=raw_text,
            expected_data=expected_data,
        )
    failed = [item for item in results if item.status == "fail"]
    high_failed = sum(1 for item in failed if item.criticality == "high")
    medium_failed = sum(1 for item in failed if item.criticality == "medium")
    low_failed = sum(1 for item in failed if item.criticality == "low")
    status = "pass"
    if high_failed > 0:
        status = "fail"
    elif medium_failed > 0 or low_failed > 0:
        status = "warn"

    summary = {
        "total": len(results),
        "passed": sum(1 for item in results if item.status == "pass"),
        "failed": len(failed),
        "warnings": sum(1 for item in results if item.status == "warn"),
        "errors": sum(1 for item in results if item.status == "error"),
        "high_failed": high_failed,
        "medium_failed": medium_failed,
        "low_failed": low_failed,
    }
    return SectionReport(
        icao=icao,
        section_id=rules.section_id,
        rules_version=rules.version,
        status=status,
        summary=summary,
        results=results,
    )


def _evaluate_table_only_section(
    *,
    actual_data: dict[str, Any],
    raw_text: str,
    expected_data: dict[str, Any] | None,
) -> list[FieldResult]:
    if not isinstance(expected_data, dict):
        return []

    expected_tables = expected_data.get("tables")
    actual_tables = actual_data.get("tables")
    if not isinstance(expected_tables, list):
        expected_tables = []
    if not isinstance(actual_tables, list):
        actual_tables = []

    results: list[FieldResult] = []
    for table_idx, expected_table in enumerate(expected_tables):
        if not isinstance(expected_table, dict):
            continue

        expected_rows = expected_table.get("rows")
        actual_table = actual_tables[table_idx] if table_idx < len(actual_tables) else {}
        actual_rows = actual_table.get("rows") if isinstance(actual_table, dict) else []
        if not isinstance(expected_rows, list):
            expected_rows = []
        if not isinstance(actual_rows, list):
            actual_rows = []

        for row_idx, expected_row in enumerate(expected_rows):
            actual_row = actual_rows[row_idx] if row_idx < len(actual_rows) else {}
            if not isinstance(expected_row, dict):
                continue
            if not isinstance(actual_row, dict):
                actual_row = {}

            key_map = _build_row_key_map(expected_row, actual_row)

            for key, expected_value in expected_row.items():
                field_path = f"tables[{table_idx}].rows[{row_idx}].{key}"
                actual_key = key_map.get(key, key)
                actual_value = actual_row.get(actual_key)
                normalized_expected = _normalize(expected_value, ["strip", "collapse_spaces", "normalize_dash", "normalize_quotes"])
                normalized_actual = _normalize(actual_value, ["strip", "collapse_spaces", "normalize_dash", "normalize_quotes"])
                passed = _table_cell_match(normalized_expected, normalized_actual)
                evidence = _has_evidence(expected_value, raw_text)
                probable_cause = "ok" if passed else _derive_probable_cause("value_mismatch", evidence)

                results.append(
                    FieldResult(
                        field=field_path,
                        status="pass" if passed else "fail",
                        expected=expected_value,
                        actual=actual_value,
                        normalized_expected=normalized_expected,
                        normalized_actual=normalized_actual,
                        type="text",
                        criticality="high",
                        probable_cause=probable_cause,
                        evidence_in_raw_text=evidence,
                        message=(
                            "Values match."
                            if passed
                            else f"Expected {expected_value!r} but got {actual_value!r}."
                        ),
                    )
                )
    return results


def _build_row_key_map(expected_row: dict[str, Any], actual_row: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    actual_keys = [k for k in actual_row.keys() if isinstance(k, str)]
    used: set[str] = set()
    for expected_key in expected_row.keys():
        if expected_key in actual_row:
            mapping[expected_key] = expected_key
            used.add(expected_key)
            continue
        best_key = None
        best_score = 0.0
        for actual_key in actual_keys:
            if actual_key in used:
                continue
            score = SequenceMatcher(
                a=_normalize_for_semantic_compare(expected_key),
                b=_normalize_for_semantic_compare(actual_key),
            ).ratio()
            if score > best_score:
                best_score = score
                best_key = actual_key
        if best_key is not None and best_score >= 0.7:
            mapping[expected_key] = best_key
            used.add(best_key)
    return mapping


def _table_cell_match(expected: Any, actual: Any) -> bool:
    if expected == actual:
        return True
    if isinstance(expected, str) and isinstance(actual, str):
        return _semantic_text_equal(expected, actual)
    return False


def _resolve_path(payload: dict[str, Any], field_path: str) -> Any:
    if re.fullmatch(r"item_\d+", field_path):
        return _resolve_field_value(payload, field_path)

    current: Any = payload
    tokens = field_path.replace("]", "").replace("[", ".").split(".")
    for token in (part for part in tokens if part):
        if isinstance(current, list):
            if not token.isdigit():
                return _MISSING
            index = int(token)
            if index >= len(current):
                return _MISSING
            current = current[index]
            continue
        if not isinstance(current, dict) or token not in current:
            return _MISSING
        current = current[token]
    return current


def _resolve_field_value(payload: dict[str, Any], field_id: str) -> Any:
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return _MISSING
    for entry in fields:
        if not isinstance(entry, dict):
            continue
        if entry.get("field") == field_id:
            return entry.get("value", _MISSING)
    return _MISSING


def _resolve_field_value_by_label(payload: dict[str, Any], label: str) -> Any:
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return _MISSING
    target = _normalize_field_token(label)
    for entry in fields:
        if not isinstance(entry, dict):
            continue
        current_label = entry.get("label")
        if not isinstance(current_label, str):
            continue
        if _normalize_field_token(current_label) == target:
            return entry.get("value", _MISSING)
    return _MISSING


def _resolve_field_value_by_expected(payload: dict[str, Any], expected_value: Any) -> Any:
    if not isinstance(expected_value, str):
        return _MISSING
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return _MISSING

    target = _normalize_for_semantic_compare(expected_value)
    for entry in fields:
        if not isinstance(entry, dict):
            continue
        value = entry.get("value")
        if not isinstance(value, str):
            continue
        candidate = _normalize_for_semantic_compare(value)
        if not candidate:
            continue
        if candidate == target or target in candidate or candidate in target:
            return value
    return _MISSING


def _normalize_field_token(value: str) -> str:
    text = _normalize_for_evidence(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize(value: Any, operations: list[str]) -> Any:
    if not isinstance(value, str):
        return value

    normalized = value
    for op in operations:
        if op == "strip":
            normalized = normalized.strip()
        elif op == "collapse_spaces":
            normalized = " ".join(normalized.split())
        elif op == "uppercase":
            normalized = normalized.upper()
        elif op == "lowercase":
            normalized = normalized.lower()
        elif op == "normalize_dash":
            normalized = normalized.replace("–", "-").replace("—", "-")
        elif op == "normalize_quotes":
            normalized = normalized.replace("’", "'").replace("`", "'").replace("\"", '"')
        elif op == "normalize_units":
            normalized = _normalize_units_text(normalized)
        elif op == "normalize_diacritics":
            normalized = _strip_diacritics(normalized)
        elif op == "normalize_slash_spacing":
            normalized = _normalize_slash_spacing(normalized)
        elif op == "normalize_bullets":
            normalized = _normalize_bullets(normalized)
        elif op == "normalize_table_punctuation":
            normalized = _normalize_table_punctuation(normalized)
    return normalized


def _compare(rule: FieldRule, expected: Any, actual: Any) -> tuple[bool, str]:
    if rule.type == "object_presence":
        exists = actual is not None
        expected_bool = bool(expected)
        return (exists == expected_bool, "ok" if exists == expected_bool else "value_mismatch")
    if rule.type == "nullability":
        should_be_null = bool(expected)
        is_null = actual is None
        return (is_null == should_be_null, "ok" if is_null == should_be_null else "value_mismatch")
    if rule.type == "bool":
        return (bool(actual) == bool(expected), "ok" if bool(actual) == bool(expected) else "value_mismatch")
    if rule.type == "array_text":
        if not isinstance(actual, list) or not isinstance(expected, list):
            return False, "schema_mismatch"
        actual_values = [str(item) for item in actual]
        expected_values = [str(item) for item in expected]
        return (
            sorted(actual_values) == sorted(expected_values),
            "ok" if sorted(actual_values) == sorted(expected_values) else "value_mismatch",
        )
    if rule.type == "number":
        try:
            expected_value = float(expected)
            actual_value = float(actual)
        except (TypeError, ValueError):
            return False, "schema_mismatch"
        tolerance = rule.tolerance if rule.tolerance is not None else 0.0
        if abs(expected_value - actual_value) <= tolerance:
            return True, "ok"
        return False, "numeric_tolerance_exceeded"
    if expected == actual:
        return True, "ok"
    if isinstance(expected, str) and isinstance(actual, str) and _semantic_text_equal(expected, actual):
        return True, "ok"
    return False, "value_mismatch"


def _has_evidence(expected: Any, raw_text: str) -> bool:
    if not isinstance(expected, str):
        return False
    expected_norm = " ".join(_normalize_for_evidence(expected).split())
    raw_norm = " ".join(_normalize_for_evidence(raw_text).split())
    return expected_norm in raw_norm


def _derive_probable_cause(compare_cause: str, evidence_in_raw_text: bool) -> str:
    if compare_cause == "schema_mismatch":
        return "schema_mismatch"
    if compare_cause == "numeric_tolerance_exceeded":
        return "numeric_tolerance_exceeded"
    if compare_cause == "value_mismatch" and evidence_in_raw_text:
        return "normalization_gap"
    if not evidence_in_raw_text:
        return "raw_missing"
    return "prompt_extraction_gap"


def _strip_diacritics(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _normalize_units_text(value: str) -> str:
    # Keep commas/periods untouched; only normalize spacing around common units/symbols.
    text = re.sub(r"\s*°\s*", "° ", value)
    text = re.sub(r"(?<=\d)\s*(km|m|ft|hr|utc)\b", r" \1", text, flags=re.IGNORECASE)
    return " ".join(text.split())


def _normalize_for_evidence(value: str) -> str:
    # Evidence lookup should be resilient to punctuation/diacritics/OCR dash noise.
    v = value.upper().replace("–", "-").replace("—", "-").replace("’", "'")
    v = _strip_diacritics(v)
    return v


def _semantic_text_equal(expected: str, actual: str) -> bool:
    left = _normalize_for_semantic_compare(expected)
    right = _normalize_for_semantic_compare(actual)
    if left == right:
        return True
    if not left or not right:
        return False
    if left in right or right in left:
        shorter = min(len(left), len(right))
        longer = max(len(left), len(right))
        if longer > 0 and (shorter / longer) >= 0.6:
            return True
    # Accept small orthographic differences (e.g., OCR/fixtures accents/diacritics variants).
    return SequenceMatcher(a=left, b=right).ratio() >= 0.95


def _normalize_for_semantic_compare(value: str) -> str:
    text = _normalize_for_evidence(value)
    text = text.replace("°", " DEG ")
    text = text.replace("º", " DEG ")
    text = text.replace("'", " ")
    text = text.replace('"', " ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = text.replace("(", " ").replace(")", " ")
    text = re.sub(r"[^A-Z0-9.+]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_slash_spacing(value: str) -> str:
    value = re.sub(r"(?<=\S)\s*/\s+(?=\S)", " / ", value)
    value = re.sub(r"(?<=\S)\s+/\s*(?=\S)", " / ", value)
    return " ".join(value.split())


def _normalize_bullets(value: str) -> str:
    return re.sub(r"(?m)^\s*[-*]\s+", "", value)


def _normalize_table_punctuation(value: str) -> str:
    value = value.replace("\u2013", "-").replace("\u2014", "-")
    value = re.sub(r"\s*\|\s*", " | ", value)
    return " ".join(value.split())
