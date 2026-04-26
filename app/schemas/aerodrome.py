from pydantic import BaseModel, ConfigDict, Field, field_validator


class RunwayBase(BaseModel):
    designator: str = Field(..., examples=["05", "23"])
    length_m: int = Field(..., ge=300, le=6000)
    width_m: int | None = Field(default=None, ge=10, le=100)
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
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    elevation_ft: int | None = Field(default=None, ge=0)


class AerodromeCreate(AerodromeBase):
    runways: list[RunwayBase] = Field(default_factory=list)

    @field_validator("icao_code")
    @classmethod
    def icao_must_be_argentine(cls, v: str) -> str:
        if not v.upper().startswith("SA"):
            raise ValueError('Argentine ICAO codes must start with "SA"')
        return v.upper()


class AerodromeResponse(AerodromeBase):
    model_config = ConfigDict(from_attributes=True)

    runways: list[RunwayResponse] = Field(default_factory=list)
