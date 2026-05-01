"""Load golden expected/rules fixtures from tests/golden."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.unit.golden.contracts import FieldRule, RulesFile

GOLDEN_DIR = Path(__file__).resolve().parents[2] / "golden"


def section_id_to_filename_stem(section_id: str) -> str:
    return section_id.replace(" ", "_").replace(".", "_")


def load_expected(icao: str, section_id: str) -> dict[str, Any]:
    stem = section_id_to_filename_stem(section_id)
    path = GOLDEN_DIR / icao.upper() / f"{stem}.expected.json"
    if not path.exists():
        raise FileNotFoundError(f"Golden expected fixture not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_rules(icao: str, section_id: str) -> RulesFile:
    stem = section_id_to_filename_stem(section_id)
    path = GOLDEN_DIR / icao.upper() / f"{stem}.rules.json"
    if not path.exists():
        raise FileNotFoundError(f"Golden rules fixture not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    fields = [FieldRule(**field_payload) for field_payload in payload.get("fields", [])]
    return RulesFile(section_id=payload["section_id"], version=payload["version"], fields=fields)


def validate_fixture_is_meaningful(expected_data: dict[str, Any], rules: RulesFile) -> None:
    has_expected_tables = bool(expected_data.get("tables")) if isinstance(expected_data, dict) else False
    if not rules.fields and not has_expected_tables:
        raise ValueError("Golden fixture invalid: rules.json has no fields and expected has no tables")

    only_presence_rules = bool(rules.fields) and all(rule.type == "object_presence" for rule in rules.fields)
    if only_presence_rules:
        raise ValueError(
            "Golden fixture too weak: all rules are object_presence. "
            "Add literal field expectations (text/number/array_text/bool/nullability)."
        )

    if _is_expected_effectively_empty(expected_data):
        raise ValueError(
            "Golden fixture invalid: expected.json is empty or placeholder. "
            "Provide real expected values for this section."
        )


def _is_expected_effectively_empty(payload: dict[str, Any]) -> bool:
    if not payload:
        return True
    values = list(payload.values())
    if len(values) == 1 and isinstance(values[0], dict) and len(values[0]) == 0:
        return True
    return False
