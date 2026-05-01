"""Generic AD 2.x section schema registry aligned to golden format."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class FieldValue(BaseModel):
    field: str
    label: str
    value: str | None = None


class GenericTable(BaseModel):
    name: str
    label: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, str | None]] = Field(default_factory=list)


class GenericAd2SectionData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    section_id: str
    schema_name: str = Field(alias="schema")
    fields: list[FieldValue] = Field(default_factory=list)
    tables: list[GenericTable] = Field(default_factory=list)


SECTION_SCHEMA_REGISTRY: dict[str, tuple[str, type[BaseModel]]] = {
    f"AD 2.{idx}": ("__self__", GenericAd2SectionData) for idx in range(1, 26)
}
