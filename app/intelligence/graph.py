"""LangGraph definition for the intelligence orchestrator."""

from __future__ import annotations

import logging
import asyncio
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from app.intelligence.aerodrome_intel_service import get_aerodrome_intelligence
from app.intelligence.contracts import (
    AerodromeIntelResult,
    Alert,
    NotamIntelResult,
    OrchestratorRequest,
)
from app.intelligence.notam_intel_service import get_notam_intelligence

logger = logging.getLogger(__name__)


class IntelligenceState(TypedDict, total=False):
    request: OrchestratorRequest
    aerodrome_result: AerodromeIntelResult
    notam_result: NotamIntelResult
    alerts: list[Alert]
    intent: str


def _route_intents(state: IntelligenceState) -> str:
    request = state["request"]
    if request.aerodrome is not None and request.notam is not None:
        return "both_node"
    if request.aerodrome is not None:
        return "aerodrome_node"
    if request.notam is not None:
        return "notam_node"
    return "aggregate_results"


def _route_intents_node(_: IntelligenceState) -> IntelligenceState:
    return {}


async def _aerodrome_node(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    intent = request.aerodrome
    if intent is None:
        return {}
    result = await get_aerodrome_intelligence(
        intent.icao,
        force_refresh=intent.force_refresh,
        section_ids=intent.section_ids,
    )
    return {"aerodrome_result": result}


async def _notam_node(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    intent = request.notam
    if intent is None:
        return {}
    result = await get_notam_intelligence(intent.icao, force_refresh=intent.force_refresh)
    return {"notam_result": result}


async def _both_node(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    aerodrome_intent = request.aerodrome
    notam_intent = request.notam
    if aerodrome_intent is None or notam_intent is None:
        return {}

    aerodrome_result, notam_result = await asyncio.gather(
        get_aerodrome_intelligence(
            aerodrome_intent.icao,
            force_refresh=aerodrome_intent.force_refresh,
            section_ids=aerodrome_intent.section_ids,
        ),
        get_notam_intelligence(notam_intent.icao, force_refresh=notam_intent.force_refresh),
    )
    return {
        "aerodrome_result": aerodrome_result,
        "notam_result": notam_result,
    }


def _aggregate_results(state: IntelligenceState) -> IntelligenceState:
    request = state["request"]
    alerts: list[Alert] = []

    aerodrome_result = state.get("aerodrome_result")
    if aerodrome_result is not None:
        alerts.extend(aerodrome_result.alerts)

    notam_result = state.get("notam_result")
    if notam_result is not None:
        alerts.extend(notam_result.alerts)

    parts = []
    if request.aerodrome is not None:
        parts.append("aerodrome_context")
    if request.notam is not None:
        parts.append("notam_context")

    return {
        "alerts": alerts,
        "intent": "+".join(parts) if parts else "noop",
    }


def build_graph():
    graph = StateGraph(IntelligenceState)
    graph.add_node("route_intents", _route_intents_node)
    graph.add_node("aerodrome_node", _aerodrome_node)
    graph.add_node("notam_node", _notam_node)
    graph.add_node("both_node", _both_node)
    graph.add_node("aggregate_results", _aggregate_results)

    graph.add_edge(START, "route_intents")
    graph.add_conditional_edges(
        "route_intents",
        _route_intents,
        ["aerodrome_node", "notam_node", "both_node", "aggregate_results"],
    )
    graph.add_edge("aerodrome_node", "aggregate_results")
    graph.add_edge("notam_node", "aggregate_results")
    graph.add_edge("both_node", "aggregate_results")
    graph.add_edge("aggregate_results", END)
    return graph.compile()


intelligence_graph = build_graph()
