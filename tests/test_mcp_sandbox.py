"""Fail-closed MCP capability and sandbox-bound tool tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend.mcp import TOOLS, AssetReliabilityMCP
from backend.models import IncidentDetail


def _incident() -> IncidentDetail:
    return IncidentDetail(
        id="incident-a",
        sandbox_id="sandbox-a",
        scenario_id="scenario-a",
        asset_id="P-204",
        title="Bearing risk",
        condition="Persistent vibration",
        trigger_prompt="Assess the alert",
    )


@pytest.mark.asyncio
async def test_mcp_rejects_missing_unknown_and_cross_incident_contexts() -> None:
    repository = SimpleNamespace(
        resolve_run_context=AsyncMock(return_value=None),
        incident=AsyncMock(return_value=_incident()),
    )
    mcp = AssetReliabilityMCP(repository)

    with pytest.raises(ValueError, match="Missing internal"):
        await mcp.call_tool("prepare_work_order", {"asset_id": "P-204"})
    with pytest.raises(ValueError, match="Unknown or terminal"):
        await mcp.call_tool(
            "prepare_work_order",
            {"asset_id": "P-204", "incident_id": "incident-a", "_run_context": "bad"},
        )

    repository.resolve_run_context.return_value = {
        "sandbox_id": "sandbox-a",
        "scenario_id": "scenario-a",
        "incident_id": "incident-a",
        "run_id": "run-a",
    }
    with pytest.raises(ValueError, match="Cross-incident"):
        await mcp.call_tool(
            "prepare_work_order",
            {"asset_id": "P-204", "incident_id": "incident-b", "_run_context": "valid"},
        )


@pytest.mark.asyncio
async def test_mcp_resolves_asset_data_only_inside_run_context_sandbox() -> None:
    pump = SimpleNamespace(asset_id="P-204")
    repository = SimpleNamespace(
        resolve_run_context=AsyncMock(
            return_value={
                "sandbox_id": "sandbox-a",
                "scenario_id": "scenario-a",
                "incident_id": "incident-a",
                "run_id": "run-a",
            }
        ),
        incident=AsyncMock(return_value=_incident()),
        snapshot=AsyncMock(return_value=SimpleNamespace(pumps=[pump])),
    )
    result = await AssetReliabilityMCP(repository).call_tool(
        "prepare_work_order",
        {
            "asset_id": "P-204",
            "incident_id": "incident-a",
            "action": "Replace bearing",
            "_run_context": "valid",
        },
    )

    assert result["incident_id"] == "incident-a"
    repository.snapshot.assert_awaited_once_with("sandbox-a")


def test_every_source_tool_requires_runtime_injected_context() -> None:
    assert all("_run_context" in tool["inputSchema"]["required"] for tool in TOOLS)
