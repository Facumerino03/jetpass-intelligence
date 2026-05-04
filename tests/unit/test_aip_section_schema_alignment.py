from app.services.enrichment.aip_section_schemas import SECTION_SCHEMA_REGISTRY


def test_schema_registry_uses_generic_section_model_for_all_ad2_sections() -> None:
    for idx in range(1, 26):
        assert SECTION_SCHEMA_REGISTRY[f"AD 2.{idx}"][0] == "__self__"
