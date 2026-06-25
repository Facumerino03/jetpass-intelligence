"""Mandatory Field 18 indicators derived from FPL fields (RAAC Parte 91 Apéndice M)."""

from __future__ import annotations

from dataclasses import dataclass, field

from app.intelligence.contracts import (
    Alert,
    AlertLevel,
    Field18AerodromeContexts,
    Field18Suggestion,
    FlightPlanFields,
    FplAerodromeContext,
    FplFieldUpdate,
)
from app.intelligence.fpl_rules.coordinates import format_oaci_coordinates

# Canonical Field 18 indicator order per RAAC Parte 91 (subset used in phase 1).
FIELD18_INDICATOR_ORDER: tuple[str, ...] = (
    "DEP/",
    "DEST/",
    "REG/",
    "TYP/",
    "ALTN/",
)

_AERODROME_SLOTS: tuple[tuple[str, str, str, str], ...] = (
    ("departure", "departure_aerodrome", "DEP/", "Casilla 13"),
    ("destination", "destination_aerodrome", "DEST/", "Casilla 16"),
    ("alternate_1", "alternate_aerodrome_1", "ALTN/", "Casilla 16 (alternativa 1)"),
    ("alternate_2", "alternate_aerodrome_2", "ALTN/", "Casilla 16 (alternativa 2)"),
)


@dataclass
class Field18DeterministicResult:
    suggestions: list[Field18Suggestion] = field(default_factory=list)
    fpl_updates: list[FplFieldUpdate] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)


def _normalize_code(value: str | None) -> str:
    return value.strip().upper() if isinstance(value, str) else ""


def _normalize_text(value: str | None) -> str:
    return value.strip() if isinstance(value, str) else ""


def _add_suggestion(
    results: list[Field18Suggestion],
    *,
    indicator: str,
    value: str,
    reason: str,
) -> None:
    cleaned = value.strip()
    if not cleaned:
        return
    results.append(
        Field18Suggestion(
            indicator=indicator,
            suggested_value=cleaned,
            full_field=f"{indicator}{cleaned}",
            reason=reason,
            is_mandatory=True,
            confidence="high",
        )
    )


def _sort_suggestions(results: list[Field18Suggestion]) -> list[Field18Suggestion]:
    order_index = {indicator: idx for idx, indicator in enumerate(FIELD18_INDICATOR_ORDER)}
    return sorted(results, key=lambda item: order_index.get(item.indicator, 999))


def _legacy_zzzz_indicators(fields: FlightPlanFields) -> list[Field18Suggestion]:
    """Indicators when the FPL field already contains ZZZZ or AFIL (backend pre-filled detail)."""
    results: list[Field18Suggestion] = []

    aircraft_type = _normalize_code(fields.aircraft_type)
    if aircraft_type == "ZZZZ":
        _add_suggestion(
            results,
            indicator="TYP/",
            value=_normalize_text(fields.typ_detail),
            reason="Casilla 9 contiene ZZZZ: se debe indicar el tipo de aeronave en TYP/.",
        )

    departure = _normalize_code(fields.departure_aerodrome)
    if departure in {"ZZZZ", "AFIL"}:
        _add_suggestion(
            results,
            indicator="DEP/",
            value=_normalize_text(fields.dep_detail),
            reason=(
                f"Casilla 13 contiene {departure}: se debe indicar el lugar de salida en DEP/."
            ),
        )

    destination = _normalize_code(fields.destination_aerodrome)
    if destination == "ZZZZ":
        _add_suggestion(
            results,
            indicator="DEST/",
            value=_normalize_text(fields.dest_detail),
            reason="Casilla 16 contiene ZZZZ: se debe indicar el lugar de destino en DEST/.",
        )

    alternate_1 = _normalize_code(fields.alternate_aerodrome_1)
    alternate_2 = _normalize_code(fields.alternate_aerodrome_2)
    if alternate_1 == "ZZZZ" or alternate_2 == "ZZZZ":
        _add_suggestion(
            results,
            indicator="ALTN/",
            value=_normalize_text(fields.altn_detail),
            reason="Casilla 16 contiene ZZZZ en alternativa: se debe indicar el aeródromo en ALTN/.",
        )

    identification = _normalize_text(fields.aircraft_identification)
    registration = _normalize_text(fields.registration)
    if registration and registration.upper() != identification.upper():
        _add_suggestion(
            results,
            indicator="REG/",
            value=registration,
            reason="La matrícula difiere del identificador de aeronave de la Casilla 7.",
        )

    return results


def _is_legacy_placeholder(code: str, *, slot_key: str) -> bool:
    if code == "ZZZZ":
        return True
    return slot_key == "departure" and code == "AFIL"


def _get_aerodrome_context(
    aerodromes: Field18AerodromeContexts | None,
    slot_key: str,
) -> FplAerodromeContext | None:
    if aerodromes is None:
        return None
    return getattr(aerodromes, slot_key, None)


def _non_controlled_indicators(
    fields: FlightPlanFields,
    aerodromes: Field18AerodromeContexts | None,
) -> Field18DeterministicResult:
    """Indicators and FPL patches for non-controlled aerodromes with a local 3-letter code."""
    result = Field18DeterministicResult()

    for slot_key, field_name, indicator, casilla_label in _AERODROME_SLOTS:
        code = _normalize_code(getattr(fields, field_name, None))
        if not code or _is_legacy_placeholder(code, slot_key=slot_key):
            continue

        context = _get_aerodrome_context(aerodromes, slot_key)
        if context is None or context.is_controlled:
            continue

        if len(code) != 3 or not code.isalpha():
            continue

        local_code = _normalize_code(context.local_identifier)
        if not local_code:
            result.alerts.append(
                Alert(
                    code="field18_missing_local_code",
                    message=(
                        f"{casilla_label}: aeródromo no controlado sin código local "
                        f"para derivar {indicator}."
                    ),
                    level=AlertLevel.ERROR,
                )
            )
            continue

        try:
            coords = format_oaci_coordinates(context.latitude, context.longitude)
        except (TypeError, ValueError):
            result.alerts.append(
                Alert(
                    code="field18_missing_coordinates",
                    message=(
                        f"{casilla_label}: coordenadas inválidas para aeródromo "
                        f"no controlado {local_code}."
                    ),
                    level=AlertLevel.ERROR,
                )
            )
            continue

        detail_value = f"{local_code}{coords}"
        _add_suggestion(
            result.suggestions,
            indicator=indicator,
            value=detail_value,
            reason=(
                f"{casilla_label} contiene aeródromo no controlado ({local_code}): "
                f"se debe indicar en {indicator} y reemplazar la casilla por ZZZZ."
            ),
        )
        result.fpl_updates.append(
            FplFieldUpdate(
                field=field_name,  # type: ignore[arg-type]
                from_value=code,
                to_value="ZZZZ",
                reason=(
                    f"Aeródromo no controlado: el código local y coordenadas van en "
                    f"{indicator} de Casilla 18."
                ),
            )
        )

    return result


def compute_mandatory_indicators(
    fields: FlightPlanFields,
    aerodromes: Field18AerodromeContexts | None = None,
) -> Field18DeterministicResult:
    """Return mandatory Field 18 entries and suggested FPL updates for current field values."""
    legacy = _legacy_zzzz_indicators(fields)
    non_controlled = _non_controlled_indicators(fields, aerodromes)

    combined_suggestions = _sort_suggestions(legacy + non_controlled.suggestions)
    return Field18DeterministicResult(
        suggestions=combined_suggestions,
        fpl_updates=non_controlled.fpl_updates,
        alerts=non_controlled.alerts,
    )
