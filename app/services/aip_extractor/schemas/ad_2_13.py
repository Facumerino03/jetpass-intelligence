"""AD 2.13 — Declared distances."""

from pydantic import BaseModel, Field


class DeclaredDistanceEntry(BaseModel):
    """Declared distances for one RWY or intersection take-off point."""

    rwy_designator: str | None = Field(
        None,
        description=(
            "RWY designator, may include intersection for reduced-length take-offs. "
            "Examples: '17', '11', '17 Intersec. TWY B', '35 Intersec. RWY 11-29'."
        ),
    )
    tora_m: float | None = Field(
        None,
        description="Take-off Run Available in metres. Numeric value only.",
    )
    toda_m: float | None = Field(
        None,
        description="Take-off Distance Available in metres. Numeric value only.",
    )
    asda_m: float | None = Field(
        None,
        description="Accelerate-Stop Distance Available in metres. Numeric value only.",
    )
    lda_m: float | None = Field(
        None,
        description=(
            "Landing Distance Available in metres. Numeric value only. "
            "Set to null if value is 'NU' (Not Usable); use lda_not_usable=true in that case."
        ),
    )
    lda_not_usable: bool = Field(
        False,
        description=(
            "True when LDA is declared 'NU' (Not Usable / No Utilizable), "
            "typically for intersection take-offs where landing is not applicable."
        ),
    )
    remarks: str | None = None


class DeclaredDistances(BaseModel):
    """AD 2.13 DISTANCIAS DECLARADAS."""

    entries: list[DeclaredDistanceEntry] = Field(default_factory=list)
