"""LangGraph definition for the intelligence orchestrator."""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from app.intelligence.aerodrome_intel_service import get_aerodrome_intelligence
from app.intelligence.contracts import (
    AerodromeIntelResult,
    Alert,
    GeoCoords,
    NotamIntelResult,
    OrchestratorRequest,
    WeatherIntelResult,
)
from app.intelligence.geo.geo_service import get_aerodrome_geo_intelligence
from app.intelligence.notam_intel_service import get_notam_intelligence
from app.intelligence.weather_intel_service import get_weather_intelligence


class IntelligenceState(TypedDict, total=False):
    request: OrchestratorRequest
    aerodrome_result: AerodromeIntelResult
    notam_result: NotamIntelResult
    weather_result: WeatherIntelResult
    aerodrome_geo_result: dict[str, GeoCoords]
    alerts: list[Alert]
    intent: str


async def _run_requested_capabilities(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    calls: list[tuple[str, Any]] = []

    if request.aerodrome is not None:
        aero = request.aerodrome
        calls.append((
            "aerodrome_result",
            get_aerodrome_intelligence(
                aero.icao,
                force_refresh=aero.force_refresh,
            ),
        ))
    if request.notam is not None:
        ntm = request.notam
        calls.append(("notam_result", get_notam_intelligence(ntm.icao, force_refresh=ntm.force_refresh)))
    if request.weather is not None:
        wx = request.weather
        calls.append((
            "weather_result",
            get_weather_intelligence(
                wx.icao,
                force_refresh=wx.force_refresh,
                metar_hours_back=wx.metar_hours_back,
            ),
        ))
    if request.aerodrome_geo is not None:
        calls.append((
            "aerodrome_geo_result",
            get_aerodrome_geo_intelligence(request.aerodrome_geo),
        ))

    if not calls:
        return {}

    results = await asyncio.gather(*(call for _, call in calls))
    return {key: result for (key, _), result in zip(calls, results, strict=True)}


def _aggregate_results(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    alerts: list[Alert] = []

    for key in ("aerodrome_result", "notam_result", "weather_result"):
        result = state.get(key)
        if result is not None:
            alerts.extend(result.alerts)

    parts = []
    if request.aerodrome is not None:
        parts.append("aerodrome_context")
    if request.notam is not None:
        parts.append("notam_context")
    if request.weather is not None:
        parts.append("weather_context")
    if request.aerodrome_geo is not None:
        parts.append("aerodrome_geo")

    return {"alerts": alerts, "intent": "+".join(parts) if parts else "noop"}


def build_graph():
    graph = StateGraph(IntelligenceState)
    graph.add_node("run_requested_capabilities", _run_requested_capabilities)
    graph.add_node("aggregate_results", _aggregate_results)

    graph.add_edge(START, "run_requested_capabilities")
    graph.add_edge("run_requested_capabilities", "aggregate_results")
    graph.add_edge("aggregate_results", END)
    return graph.compile()


intelligence_graph = build_graph()
