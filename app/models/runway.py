from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.aerodrome import Aerodrome
from app.models.base import Base, TimestampedUUIDMixin


class Runway(TimestampedUUIDMixin, Base):
    __tablename__ = "runway"
    __table_args__ = {"schema": "aip"}

    aerodrome_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("aip.aerodrome.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    designator: Mapped[str] = mapped_column(String(10), nullable=False)
    length_m: Mapped[int] = mapped_column(Integer, nullable=False)
    width_m: Mapped[int | None] = mapped_column(Integer, nullable=True)
    surface_type: Mapped[str | None] = mapped_column(String(30), nullable=True)

    aerodrome: Mapped[Aerodrome] = relationship(back_populates="runways")
