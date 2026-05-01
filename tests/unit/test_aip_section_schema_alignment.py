from __future__ import annotations

import json
from pathlib import Path

from app.services.enrichment.aip_section_schemas import SECTION_SCHEMA_REGISTRY


def _load_expected(section_id: str) -> dict:
    stem = section_id.replace(" ", "_").replace(".", "_")
    path = Path("tests/golden/SAMR") / f"{stem}.expected.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_schema_registry_matches_critical_golden_roots() -> None:
    for idx in range(1, 26):
        assert SECTION_SCHEMA_REGISTRY[f"AD 2.{idx}"][0] == "__self__"


def test_critical_section_models_accept_samr_expected_payloads() -> None:
    for section_id in ("AD 2.2", "AD 2.12", "AD 2.14", "AD 2.18", "AD 2.24"):
        _, model = SECTION_SCHEMA_REGISTRY[section_id]
        payload = _load_expected(section_id)
        parsed = model.model_validate(payload)
        assert parsed.model_dump(mode="json")
