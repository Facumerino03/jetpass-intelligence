from pydantic import BaseModel, ConfigDict, Field


class RunwayBase(BaseModel):
    designator: str = Field(..., examples=["05", "23"])
    length_m: int = Field(..., ge=1)
    width_m: int | None = Field(default=None, ge=1)
    surface_type: str | None = Field(default=None, examples=["ASPH", "CONC"])


class RunwayResponse(RunwayBase):
    model_config = ConfigDict(from_attributes=True)


class AerodromeBase(BaseModel):
    icao_code: str = Field(..., min_length=4, max_length=4, examples=["SAMR"])
    iata_code: str | None = Field(default=None, min_length=3, max_length=3)
    name: str = Field(..., min_length=1)
    city: str | None = None
    province: str | None = None
    country: str = Field(default="Argentina")
    latitude: float
    longitude: float
    elevation_ft: int | None = None


class AerodromeCreate(AerodromeBase):
    runways: list[RunwayBase] = Field(default_factory=list)


class AerodromeResponse(AerodromeBase):
    model_config = ConfigDict(from_attributes=True)

    runways: list[RunwayResponse] = Field(default_factory=list)
