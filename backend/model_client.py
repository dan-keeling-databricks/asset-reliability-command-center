"""Thin asynchronous client for Foundation Model APIs governed by Unity Gateway."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class ModelResult:
    content: str
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True)
class ModelTurn:
    message: dict[str, Any]
    input_tokens: int
    output_tokens: int


class DatabricksModelClient:
    """Normalize Unity Gateway Responses API output into the runtime's chat contract."""

    def __init__(self, host: str, client: httpx.AsyncClient | None = None) -> None:
        self.host = host.rstrip("/")
        if self.host and not self.host.startswith(("http://", "https://")):
            self.host = f"https://{self.host}"
        self._client = client or httpx.AsyncClient(timeout=60)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def chat(
        self, endpoint: str, messages: list[dict[str, str]], token: str | None = None
    ) -> ModelResult:
        turn = await self.turn(endpoint, messages, token=token)
        return ModelResult(
            content=str(turn.message.get("content") or ""),
            input_tokens=turn.input_tokens,
            output_tokens=turn.output_tokens,
        )

    async def turn(
        self,
        model_service: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        token: str | None = None,
    ) -> ModelTurn:
        """Execute one governed model turn through a Unity Gateway Model Service."""

        if token:
            headers = {"Authorization": f"Bearer {token}"}
        else:
            from databricks.sdk import WorkspaceClient

            headers = await asyncio.to_thread(WorkspaceClient().config.authenticate)
        payload: dict[str, Any] = {
            "model": model_service,
            "input": _responses_input(messages),
            "max_output_tokens": 450,
            "temperature": 0.1,
        }
        if tools:
            payload["tools"] = _responses_tools(tools)
            payload["tool_choice"] = "auto"
        response = await self._client.post(
            f"{self.host}/ai-gateway/mlflow/v1/responses",
            headers={**(headers or {}), "Content-Type": "application/json"},
            json=payload,
        )
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        message = _chat_message(result)
        usage = result.get("usage", {})
        return ModelTurn(
            message=message,
            input_tokens=int(usage.get("input_tokens", usage.get("prompt_tokens", 0))),
            output_tokens=int(usage.get("output_tokens", usage.get("completion_tokens", 0))),
        )


def _responses_input(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate the runtime's chat history into OpenAI Responses API input items."""

    items: list[dict[str, Any]] = []
    for message in messages:
        role = message.get("role")
        if role == "tool":
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": message["tool_call_id"],
                    "output": str(message.get("content") or ""),
                }
            )
            continue
        content = str(message.get("content") or "")
        if content:
            items.append({"role": role, "content": content})
        for tool_call in message.get("tool_calls", []):
            function = tool_call.get("function", {})
            arguments = function.get("arguments") or "{}"
            if not isinstance(arguments, str):
                arguments = json.dumps(arguments)
            items.append(
                {
                    "type": "function_call",
                    "call_id": tool_call["id"],
                    "name": function["name"],
                    "arguments": arguments,
                }
            )
    return items


def _responses_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten Chat Completions function declarations for the Responses API."""

    return [
        {
            "type": "function",
            "name": tool["function"]["name"],
            "description": tool["function"].get("description", ""),
            "parameters": tool["function"].get("parameters", {"type": "object"}),
        }
        for tool in tools
    ]


def _chat_message(payload: dict[str, Any]) -> dict[str, Any]:
    """Collapse Responses API output items into the runtime's chat-message shape."""

    content: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    for item in payload.get("output", []):
        if item.get("type") == "message":
            for part in item.get("content", []):
                if part.get("type") in {"output_text", "text"} and part.get("text"):
                    content.append(str(part["text"]))
        elif item.get("type") == "function_call":
            arguments = item.get("arguments") or "{}"
            if not isinstance(arguments, str):
                arguments = json.dumps(arguments)
            tool_calls.append(
                {
                    "id": item.get("call_id") or item.get("id"),
                    "type": "function",
                    "function": {"name": item["name"], "arguments": arguments},
                }
            )
    message: dict[str, Any] = {"role": "assistant", "content": "\n".join(content)}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return message
