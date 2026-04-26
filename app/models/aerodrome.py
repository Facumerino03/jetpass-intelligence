from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampedUUIDMixin

if TYPE_CHECKING:
    from app.models.runway import Runway


class Aerodrome(TimestampedUUIDMixin, Base):
    __tablename__ = "aerodrome"
    __table_args__ = {"schema": "aip"}

    icao_code: Mapped[str] = mapped_column(
        String(4), unique=True, index=True, nullable=False
    )
    iata_code: Mapped[str | None] = mapped_column(String(3), nullable=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    province: Mapped[str | None] = mapped_column(String(120), nullable=True)
    country: Mapped[str] = mapped_column(
        String(120), default="Argentina", nullable=False
    )
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    elevation_ft: Mapped[int | None] = mapped_column(Integer, nullable=True)

    runways: Mapped[list["Runway"]] = relationship(
        back_populates="aerodrome",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
