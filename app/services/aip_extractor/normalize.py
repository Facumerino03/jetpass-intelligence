"""Normalize AIP sentinel values (NIL, etc.) in extracted JSON."""

from typing import Any

ERROR_EXTRACCION = "ERROR_EXTRACCION"

# AIP / OACI "no information" sentinels → null after normalization.
# "NU" (Not Usable) is NOT included — it has domain meaning (e.g. LDA in AD 2.13).
_NIL_STRINGS = frozenset({
    "nil",
    "n/a",
    "na",
    "none",
    "nada",
    "ninguno",
    "not applicable",
    "no aplicable",
})


def is_nil_string(value: str) -> bool:
    """True if the string is an AIP 'no item listed' sentinel, not a real value."""
    normalized = value.strip().rstrip(".").lower()
    return normalized in _NIL_STRINGS


def normalize_value(value: Any) -> Any:
    """
    Recursively normalize extracted data.

    - NIL-like strings → None
    - ERROR_EXTRACCION → preserved unchanged
    - Other values → unchanged
    """
    if value is None:
        return None

    if isinstance(value, str):
        if value.strip() == ERROR_EXTRACCION:
            return value
        if is_nil_string(value):
            return None
        return value

    if isinstance(value, list):
        return [normalize_value(item) for item in value]

    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}

    return value


def omit_none_values(value: Any) -> Any:
    """Remove keys/items whose value is None (after NIL normalization)."""
    if value is None:
        return None

    if isinstance(value, list):
        return [omit_none_values(item) for item in value]

    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized = omit_none_values(item)
            if normalized is not None:
                result[key] = normalized
        return result

    return value


def normalize_extracted_data(data: dict[str, Any], *, omit_none: bool = True) -> dict[str, Any]:
    """Apply NIL→null normalization and optionally drop null keys."""
    normalized = normalize_value(data)
    if omit_none:
        normalized = omit_none_values(normalized)
    if isinstance(normalized, dict):
        return normalized
    return data


def collect_field_errors(
    data: Any,
    *,
    section_id: str = "",
    prefix: str = "",
) -> list[dict[str, str]]:
    """
    Find fields set to ERROR_EXTRACCION after LLM extraction.

    Returns a list of {section, field, message} for the top-level field_errors array.
    """
    errors: list[dict[str, str]] = []

    if isinstance(data, str):
        if data.strip() == ERROR_EXTRACCION:
            errors.append({
                "section": section_id,
                "field": prefix or "(root)",
                "message": "Extraction could not parse this field from the source text",
            })
        return errors

    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else key
            errors.extend(collect_field_errors(value, section_id=section_id, prefix=path))

    elif isinstance(data, list):
        for index, item in enumerate(data):
            path = f"{prefix}[{index}]"
            errors.extend(collect_field_errors(item, section_id=section_id, prefix=path))

    return errors
