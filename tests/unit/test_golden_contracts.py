"""Unit tests for golden contracts used by AD 2.x evaluation."""

from __future__ import annotations

import pytest

from tests.unit.golden.contracts import FieldRule, RulesFile


def test_field_rule_requires_supported_type() -> None:
    with pytest.raises(ValueError, match="Unsupported rule type"):
        FieldRule(
            field="identity.location_indicator",
            type="unsupported",
            criticality="high",
            expected="SAMR",
        )


def test_field_rule_rejects_tolerance_for_non_number() -> None:
    with pytest.raises(ValueError, match="tolerance is only valid for number"):
        FieldRule(
            field="identity.location_indicator",
            type="text",
            criticality="high",
            expected="SAMR",
            tolerance=0,
        )


def test_rules_file_requires_section_id_format() -> None:
    with pytest.raises(ValueError, match="section_id must look like AD 2.x"):
        RulesFile(section_id="AD2.1", version="1.0", fields=[])


def test_rules_file_accepts_valid_minimal_payload() -> None:
    rules = RulesFile(
        section_id="AD 2.1",
        version="1.0",
        fields=[
            FieldRule(
                field="identity.location_indicator",
                type="text",
                criticality="high",
                expected="SAMR",
            )
        ],
    )

    assert rules.section_id == "AD 2.1"
    assert len(rules.fields) == 1
