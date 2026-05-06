"""Canonical AD 2.x section scopes used by parser persistence."""

from __future__ import annotations

from typing import Iterable, TypeVar

FULL_AD2_SECTION_IDS = tuple(f"AD 2.{idx}" for idx in range(1, 26))

OPERATIONAL_AD_SECTION_IDS = (
    "AD 2.1",
    "AD 2.2",
    "AD 2.3",
    "AD 2.4",
    "AD 2.6",
    "AD 2.8",
    "AD 2.9",
    "AD 2.12",
    "AD 2.13",
    "AD 2.14",
    "AD 2.18",
    "AD 2.19",
)

_OPERATIONAL_SET = set(OPERATIONAL_AD_SECTION_IDS)

T = TypeVar("T")


def normalize_ad_section_id(section_id: str) -> str:
    return " ".join(section_id.strip().upper().split())


def filter_operational_sections(sections: Iterable[T]) -> list[T]:
    by_id: dict[str, T] = {}
    for section in sections:
        section_id = getattr(section, "section_id", None)
        if not isinstance(section_id, str):
            continue
        normalized = normalize_ad_section_id(section_id)
        if normalized in _OPERATIONAL_SET:
            by_id.setdefault(normalized, section)
    return [by_id[sid] for sid in OPERATIONAL_AD_SECTION_IDS if sid in by_id]


def validate_operational_section_ids(section_ids: Iterable[str]) -> None:
    normalized_ids = [normalize_ad_section_id(section_id) for section_id in section_ids]
    seen = set(normalized_ids)
    missing = [sid for sid in OPERATIONAL_AD_SECTION_IDS if sid not in seen]
    unexpected = [sid for sid in normalized_ids if sid not in _OPERATIONAL_SET]
    duplicates = sorted({sid for sid in normalized_ids if normalized_ids.count(sid) > 1})

    messages: list[str] = []
    if missing:
        messages.append(f"Missing required operational AD 2.x sections: {missing}")
    if unexpected:
        messages.append(f"Unexpected non-operational AD 2.x sections: {unexpected}")
    if duplicates:
        messages.append(f"Duplicate AD 2.x sections: {duplicates}")
    if messages:
        raise ValueError("; ".join(messages))