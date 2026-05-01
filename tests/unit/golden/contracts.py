"""Contract models for golden AD 2.x evaluation files."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

ALLOWED_RULE_TYPES = {"text", "number", "array_text", "bool", "object_presence", "nullability"}
ALLOWED_CRITICALITIES = {"high", "medium", "low"}
SECTION_ID_RE = re.compile(r"^AD\s+2\.(?:[1-9]|1\d|2[0-5])$")
ALLOWED_FIELD_STATUSES = {"pass", "fail", "warn", "error", "skipped"}


@dataclass(frozen=True)
class FieldRule:
    field: str
    type: str
    expected: Any | None
    criticality: str = "high"
    label: str | None = None
    required: bool | None = None
    tolerance: float | None = None
    normalization: list[str] = field(default_factory=list)
    order_sensitive: bool | None = None
    format: str | None = None

    def __post_init__(self) -> None:
        if self.type not in ALLOWED_RULE_TYPES:
            raise ValueError(f"Unsupported rule type: {self.type}")
        if self.criticality not in ALLOWED_CRITICALITIES:
            raise ValueError(f"Unsupported criticality: {self.criticality}")
        if self.tolerance is not None and self.type != "number":
            raise ValueError("tolerance is only valid for number")


@dataclass(frozen=True)
class RulesFile:
    section_id: str
    version: str
    fields: list[FieldRule]

    def __post_init__(self) -> None:
        if not SECTION_ID_RE.match(self.section_id):
            raise ValueError("section_id must look like AD 2.x")
        if not self.version.strip():
            raise ValueError("version must not be empty")


@dataclass(frozen=True)
class FieldResult:
    field: str
    status: str
    expected: Any
    actual: Any
    normalized_expected: Any
    normalized_actual: Any
    type: str
    criticality: str
    probable_cause: str
    evidence_in_raw_text: bool
    message: str

    def __post_init__(self) -> None:
        if self.status not in ALLOWED_FIELD_STATUSES:
            raise ValueError(f"Unsupported field status: {self.status}")


@dataclass(frozen=True)
class SectionReport:
    icao: str
    section_id: str
    rules_version: str
    status: str
    summary: dict[str, int]
    results: list[FieldResult]
