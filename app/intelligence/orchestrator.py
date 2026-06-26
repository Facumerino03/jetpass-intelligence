"""Minimal orchestrator — maps business intents to intelligence capabilities.

This module is intentionally kept thin: it receives an OrchestratorRequest,
dispatches to the appropriate domain services, and returns a consolidated
OrchestratorResponse.  When multiple capabilities (weather, NOTAMs, etc.) are
added, this is the layer that coordinates them — potentially via LangGraph nodes
— without touching the underlying tools or services.
"""

from __future__ import annotations

from app.intelligence.contracts import OrchestratorRequest, OrchestratorResponse
from app.intelligence.graph import intelligence_graph


async def run(request: OrchestratorRequest) -> OrchestratorResponse:
    """Execute requested intents via the compiled LangGraph orchestrator."""
    state = await intelligence_graph.ainvoke({"request": request})
    return OrchestratorResponse(
        intent=state.get("intent", "noop"),
        aerodrome=state.get("aerodrome_result"),
        notam=state.get("notam_result"),
        weather=state.get("weather_result"),
        aerodrome_geo=state.get("aerodrome_geo_result"),
        aerodrome_catalog_sync=state.get("aerodrome_catalog_sync_result"),
        fpl_field18=state.get("fpl_field18_result"),
        aircraft_type_validate=state.get("aircraft_type_validate_result"),
        alerts=state.get("alerts", []),
    )
