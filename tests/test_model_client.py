"""Contract tests for Foundation Model API inference through Unity Gateway."""

import json

import httpx
import pytest

from backend.model_client import DatabricksModelClient


@pytest.mark.asyncio
async def test_model_turn_uses_unity_gateway_responses_api() -> None:
    captured: httpx.Request | None = None

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal captured
        captured = request
        return httpx.Response(
            200,
            json={
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call-42",
                        "name": "get_asset_telemetry",
                        "arguments": '{"asset_id":"P-204"}',
                    }
                ],
                "usage": {"input_tokens": 31, "output_tokens": 9},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    model = DatabricksModelClient("https://workspace.example", client=client)
    result = await model.turn(
        "catalog.schema.asset_reliability_agent_model",
        [{"role": "user", "content": "Investigate P-204"}],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "get_asset_telemetry",
                    "description": "Read governed telemetry",
                    "parameters": {
                        "type": "object",
                        "properties": {"asset_id": {"type": "string"}},
                    },
                },
            }
        ],
        token="forwarded-obo-token",
    )

    assert captured is not None
    assert captured.url == "https://workspace.example/ai-gateway/mlflow/v1/responses"
    assert captured.headers["authorization"] == "Bearer forwarded-obo-token"
    payload = json.loads(captured.content)
    assert payload["model"] == "catalog.schema.asset_reliability_agent_model"
    assert payload["input"] == [{"role": "user", "content": "Investigate P-204"}]
    assert payload["tools"] == [
        {
            "type": "function",
            "name": "get_asset_telemetry",
            "description": "Read governed telemetry",
            "parameters": {
                "type": "object",
                "properties": {"asset_id": {"type": "string"}},
            },
        }
    ]
    assert payload["tool_choice"] == "auto"
    assert result.input_tokens == 31
    assert result.output_tokens == 9
    assert result.message == {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": "call-42",
                "type": "function",
                "function": {
                    "name": "get_asset_telemetry",
                    "arguments": '{"asset_id":"P-204"}',
                },
            }
        ],
    }
    await client.aclose()


@pytest.mark.asyncio
async def test_model_turn_replays_tool_output_and_reads_text_response() -> None:
    request_payload: dict[str, object] = {}

    async def respond(request: httpx.Request) -> httpx.Response:
        request_payload.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "No maintenance action is needed."}
                        ],
                    }
                ],
                "usage": {"input_tokens": 44, "output_tokens": 7},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    model = DatabricksModelClient("workspace.example/", client=client)
    result = await model.turn(
        "catalog.schema.asset_reliability_agent_model",
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call-7",
                        "type": "function",
                        "function": {
                            "name": "assess_failure_risk",
                            "arguments": {"asset_id": "P-207"},
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call-7",
                "content": '{"risk":"low"}',
            },
        ],
        token="forwarded-obo-token",
    )

    assert request_payload["input"] == [
        {
            "type": "function_call",
            "call_id": "call-7",
            "name": "assess_failure_risk",
            "arguments": '{"asset_id": "P-207"}',
        },
        {
            "type": "function_call_output",
            "call_id": "call-7",
            "output": '{"risk":"low"}',
        },
    ]
    assert result.message == {
        "role": "assistant",
        "content": "No maintenance action is needed.",
    }
    await client.aclose()
