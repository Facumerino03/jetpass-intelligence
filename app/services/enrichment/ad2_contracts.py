"""Runtime loader for AD 2.x extraction contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Ad2SectionContract:
    section_id: str
    rules: dict[str, Any]
    expected: dict[str, Any]


def load_ad2_contract(icao: str, section_id: str) -> Ad2SectionContract | None:
    stem = section_id.replace(" ", "_").replace(".", "_")
    base_dir = Path(__file__).resolve().parents[3] / "tests" / "golden" / icao.upper()
    rules_path = base_dir / f"{stem}.rules.json"
    expected_path = base_dir / f"{stem}.expected.json"
    if not rules_path.exists() or not expected_path.exists():
        return None

    rules = json.loads(rules_path.read_text(encoding="utf-8"))
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    return Ad2SectionContract(section_id=section_id, rules=rules, expected=expected)
