"""Field 18 intelligence service — deterministic mandatory indicators (phase 1)."""

from __future__ import annotations

from app.intelligence.contracts import Field18Intent, Field18IntelResult, Field18Suggestion
from app.intelligence.fpl_rules.deterministic import compute_mandatory_indicators


def _assemble_field18(
    suggestions: list[Field18Suggestion],
    current: str | None,
) -> str | None:
    if not suggestions:
        if current and current.strip():
            return current.strip()
        return "0"

    parts = [suggestion.full_field for suggestion in suggestions]
    assembled = " ".join(parts)

    if current and current.strip():
        return f"{current.strip()} {assembled}".strip()

    return assembled


async def get_field18_intelligence(intent: Field18Intent) -> Field18IntelResult:
    """Compute mandatory Field 18 suggestions from the current FPL field snapshot."""
    deterministic = compute_mandatory_indicators(intent.fpl_fields, intent.aerodromes)
    computed = _assemble_field18(deterministic.suggestions, intent.current_field18)

    messages: list[str] = []
    if deterministic.suggestions:
        messages.append(
            f"Generated {len(deterministic.suggestions)} mandatory Field 18 indicator(s)."
        )
    else:
        messages.append("No mandatory Field 18 indicators required for current FPL fields.")

    if deterministic.fpl_updates:
        messages.append(
            f"Suggested {len(deterministic.fpl_updates)} FPL field update(s) for backend to apply."
        )

    return Field18IntelResult(
        suggestions=deterministic.suggestions,
        computed_field18=computed,
        fpl_updates=deterministic.fpl_updates,
        alerts=deterministic.alerts,
        messages=messages,
        metadata={"engine": "deterministic", "phase": "1b"},
    )
