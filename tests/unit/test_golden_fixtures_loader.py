"""Unit tests for golden fixtures loader."""

from __future__ import annotations

import pytest

from tests.unit.golden.contracts import FieldRule, RulesFile
from tests.unit.golden.fixtures_loader import (
    section_id_to_filename_stem,
    load_expected,
    load_rules,
    validate_fixture_is_meaningful,
)


def test_section_id_to_filename_stem() -> None:
    assert section_id_to_filename_stem("AD 2.12") == "AD_2_12"


def test_load_expected_and_rules_for_samr_ad_2_1() -> None:
    expected = load_expected("SAMR", "AD 2.1")
    rules = load_rules("SAMR", "AD 2.1")

    assert isinstance(expected, dict)
    assert rules.section_id == "AD 2.1"
    assert len(rules.fields) > 0


def test_load_expected_raises_when_missing() -> None:
    with pytest.raises(FileNotFoundError, match="Golden expected fixture not found"):
        load_expected("SAMR", "AD 2.99")


def test_validate_fixture_is_meaningful_rejects_weak_placeholder_fixture() -> None:
    expected = {"administrative": {}}
    rules = RulesFile(
        section_id="AD 2.2",
        version="1.0",
        fields=[
            FieldRule(
                field="administrative",
                type="object_presence",
                criticality="high",
                expected=True,
            )
        ],
    )
    with pytest.raises(ValueError, match="all rules are object_presence"):
        validate_fixture_is_meaningful(expected, rules)
