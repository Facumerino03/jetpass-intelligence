"""Minimal orchestrator — maps business intents to intelligence capabilities.

This module is intentionally kept thin: it receives an OrchestratorRequest,
dispatches to the appropriate domain services, and returns a consolidated
OrchestratorResponse.  When multiple capabilities (weather, NOTAMs, etc.) are
added, this is the layer that coordinates them — potentially via LangGraph nodes
— without touching the underlying tools or services.
"""

from __future__ import annotations

import logging

from app.intelligence.aerodrome_intel_service import get_aerodrome_intelligence
from app.intelligence.contracts import (
    Alert,
    OrchestratorRequest,
    OrchestratorResponse,
)

logger = logging.getLogger(__name__)


async def run(request: OrchestratorRequest) -> OrchestratorResponse:
    """Execute all requested intents and return a consolidated response.

    Args:
        request: :class:`~app.intelligence.contracts.OrchestratorRequest`
            describing what the backend core needs.

    Returns:
        :class:`~app.intelligence.contracts.OrchestratorResponse` with results
        from every dispatched capability.
    """
    aggregated_alerts: list[Alert] = []
    intent_label = _resolve_intent_label(request)

    aerodrome_result = None
    if request.aerodrome is not None:
        intent = request.aerodrome
        logger.info("Orchestrator dispatching aerodrome intent for %s", intent.icao)
        aerodrome_result = await get_aerodrome_intelligence(
            intent.icao,
            force_refresh=intent.force_refresh,
            section_ids=intent.section_ids,
        )
        aggregated_alerts.extend(aerodrome_result.alerts)

    return OrchestratorResponse(
        intent=intent_label,
        aerodrome=aerodrome_result,
        alerts=aggregated_alerts,
    )


def _resolve_intent_label(request: OrchestratorRequest) -> str:
    parts = []
    if request.aerodrome is not None:
        parts.append("aerodrome_context")
    return "+".join(parts) if parts else "noop"
