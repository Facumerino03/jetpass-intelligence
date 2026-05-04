"""Unit tests for runtime AD 2.x enrichment contracts."""

from __future__ import annotations

from app.services.enrichment.ad2_contracts import load_ad2_contract


def test_load_ad2_contract_returns_none_when_missing() -> None:
    assert load_ad2_contract("ZZZZ", "AD 2.24") is None


def test_load_ad2_contract_returns_none_without_icao_specific_contracts() -> None:
    assert load_ad2_contract("SAMR", "AD 2.24") is None
