"""Production LangGraph agent orchestration for Lakebase and Unity Gateway.

Each incident runs in its own asyncio task. An explicit graph loads the governed Skill,
discovers tools on one configured MCP service, lets the model select calls, records every
observed event, and stops immediately when Gateway returns ``ASK``. The graph has no durable
in-memory checkpoint: Lakebase remains canonical for approval state and exact-request replay.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Coroutine
from datetime import datetime
from typing import Any, TypedDict
from uuid import uuid4

import mlflow
from langgraph.graph import END, START, StateGraph
from mlflow.entities import SpanType

from backend.gateway import GatewayAsk, GovernedGatewayClient
from backend.identity import OboIdentity, OboIdentityRegistry
from backend.lakebase_repository import LakebaseCommandCenterRepository
from backend.model_client import DatabricksModelClient
from backend.models import (
    ActivityDatum,
    IncidentDetail,
    PendingApproval,
    RuntimeAction,
    RuntimeActivity,
    ScenarioCommand,
    utc_now,
)
from backend.settings import Settings

MCP_ACTIVITY_ACTOR = "asset_reliability_mcp"
INTERNAL_RUN_CONTEXT = "_run_context"


class AgentGraphState(TypedDict, total=False):
    """Ephemeral state for one incident investigation.

    Identity and its OBO token exist only while the graph is executing. They are deliberately
    excluded from Lakebase and from a LangGraph checkpointer; durable approval recovery uses the
    pending request already persisted by the repository.
    """

    incident_id: str
    sandbox_id: str
    entrypoint: str
    incident: IncidentDetail
    messages: list[dict[str, Any]]
    available: dict[str, dict[str, Any]]
    openai_tools: list[dict[str, Any]]
    tool_calls: list[dict[str, Any]]
    last_risk: dict[str, Any] | None
    action_continuation_requested: bool
    model_summary: str
    terminal_response: str | None
    pending_approval: PendingApproval
    approving_subject: str
    replay_completed: bool
    phase: str


class ProductionCommandCenterRuntime:
    """Concurrent LangGraph agent runtime over canonical Lakebase state."""

    def __init__(
        self,
        repository: LakebaseCommandCenterRepository,
        settings: Settings,
        gateway: GovernedGatewayClient | None = None,
        model: DatabricksModelClient | None = None,
    ) -> None:
        if not settings.governed_mcp_endpoint:
            raise RuntimeError("GOVERNED_MCP_ENDPOINT is required in production")
        if not settings.databricks_host:
            raise RuntimeError("DATABRICKS_HOST is required in production")
        self.repository = repository
        self.settings = settings
        self.gateway = gateway or GovernedGatewayClient(settings.governed_mcp_endpoint)
        self.skill_gateway = (
            GovernedGatewayClient(settings.governed_skill_endpoint)
            if settings.governed_skill_endpoint
            else None
        )
        self.model = model or DatabricksModelClient(settings.databricks_host)
        self.identities = OboIdentityRegistry()
        self._user_identities = OboIdentityRegistry()
        self._evaluator_task: asyncio.Task[None] | None = None
        self._run_tasks: dict[asyncio.Task[None], str] = {}
        self._agent_identities: dict[str, OboIdentity] = {}
        self._agent_run_contexts: dict[str, str] = {}
        self._sandbox_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._global_agent_limit = asyncio.Semaphore(settings.global_agent_concurrency)
        self._agent_graph = self._build_agent_graph()

    async def start(self) -> None:
        await self.repository.start()
        self._evaluator_task = asyncio.create_task(self._evaluator_loop())

    async def stop(self) -> None:
        tasks = [*self._run_tasks]
        if self._evaluator_task:
            tasks.append(self._evaluator_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.gateway.close()
        if self.skill_gateway:
            await self.skill_gateway.close()
        await self.model.close()
        await self.repository.close()

    def _spawn(self, sandbox_id: str, operation: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(operation)
        self._run_tasks[task] = sandbox_id
        task.add_done_callback(lambda completed: self._run_tasks.pop(completed, None))

    def capture_identity(self, scenario_id: str, token: str | None) -> None:
        self.identities.capture(scenario_id, token)

    def cache_user_identity(self, user_email: str, token: str | None) -> None:
        self._user_identities.capture(user_email.lower(), token)

    def bind_operator_identity(self, scenario_id: str, operator_email: str) -> bool:
        cached = self._user_identities.get(operator_email.lower())
        if not cached:
            return False
        self.identities.capture(scenario_id, cached.token)
        return True

    async def restart(self, sandbox_id: str, requested_by: str | None = None) -> str:
        """Quiesce the old scenario before atomically creating its replacement."""

        async with self._sandbox_locks[sandbox_id]:
            active = next(
                (
                    item
                    for item in await self.repository.active_sandboxes()
                    if item.id == sandbox_id
                ),
                None,
            )
            await self._cancel_active_runs(sandbox_id)
            if active:
                self.identities.discard(active.scenario_id)
            return await self.repository.restart_scenario(sandbox_id, requested_by)

    async def _cancel_active_runs(self, sandbox_id: str) -> None:
        """Cancel and await every incident task so it cannot outlive its scenario."""

        tasks = [task for task, owner in self._run_tasks.items() if owner == sandbox_id]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for task in tasks:
            self._run_tasks.pop(task, None)

    async def trigger_excursion(self, sandbox_id: str) -> str:
        snapshot = await self.repository.snapshot(sandbox_id)
        command = ScenarioCommand(
            id=str(uuid4()),
            scenario_id=snapshot.scenario.id,
            sandbox_id=sandbox_id,
            kind="start_p204_excursion",
        )
        await self.repository.enqueue_command(command)
        return command.id

    async def approve(self, incident_id: str, sandbox_id: str) -> None:
        # Serialise only the governed write/restart boundary for this sandbox. Agent
        # investigations do not acquire this lock, so independent incidents remain concurrent
        # and another user's sandbox is never held behind this submission.
        async with self._sandbox_locks[sandbox_id]:
            incident = await self.repository.incident(incident_id, sandbox_id)
            if not incident or not incident.pending_approval:
                raise ValueError("Incident is not waiting for Gateway approval")
            identity = self.identities.get(incident.scenario_id)
            approving_subject = (
                identity.subject if identity and identity.subject else None
            ) or "authenticated-operator"
            graph = getattr(self, "_agent_graph", None) or self._build_agent_graph()
            self._agent_graph = graph
            with mlflow.start_span(
                name="asset_reliability_approval_resume", span_type=SpanType.CHAIN
            ) as span:
                span.set_inputs({"incident_id": incident_id, "sandbox_id": sandbox_id})
                span.set_attribute("asset.incident_id", incident_id)
                if pump_id := getattr(incident, "asset_id", None):
                    span.set_attribute("asset.pump_id", pump_id)
                mlflow.update_current_trace(session_id=sandbox_id)
                final_state = await graph.ainvoke(
                    {
                        "entrypoint": "approval",
                        "incident_id": incident_id,
                        "sandbox_id": sandbox_id,
                        "pending_approval": incident.pending_approval,
                        "approving_subject": approving_subject,
                    },
                    config={"recursion_limit": 100},
                )
                if final_state.get("replay_completed"):
                    span.set_attribute("asset.outcome", "work_order_created")
                span.set_outputs({"phase": final_state.get("phase", "unknown")})
            trace_id = mlflow.get_last_active_trace_id()
            if trace_id:
                await self._record_trace_link(incident_id, trace_id)
            if not final_state.get("replay_completed"):
                raise ValueError("Unity Gateway approval is still pending")

    async def follow_up(self, incident_id: str, message: str, sandbox_id: str) -> None:
        incident = await self.repository.incident(incident_id, sandbox_id)
        if not incident:
            raise LookupError("Incident not found")
        await self.repository.append_conversation(incident_id, "operator", message)
        started_at = utc_now()
        result = await self.model.chat(
            self.settings.model_endpoint,
            [
                {
                    "role": "system",
                    "content": "Answer concisely from the supplied governed incident record.",
                },
                {
                    "role": "user",
                    "content": f"Incident: {incident.model_dump_json()}\nQuestion: {message}",
                },
            ],
            token=None,
        )
        completed_at = utc_now()
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="model_call",
                title="Model execution",
                actor=self.settings.model_endpoint,
                status="complete",
                status_label="Completed",
                summary=result.content,
                details=[
                    ActivityDatum(label="Input tokens", value=str(result.input_tokens)),
                    ActivityDatum(label="Output tokens", value=str(result.output_tokens)),
                    ActivityDatum(label="Purpose", value="Operator follow-up"),
                    ActivityDatum(
                        label="Estimated cost",
                        value=f"{self._model_cost(result.input_tokens, result.output_tokens):.8f}",
                    ),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
            tokens=(
                result.input_tokens,
                result.output_tokens,
                self._model_cost(result.input_tokens, result.output_tokens),
            ),
        )

    async def _evaluator_loop(self) -> None:
        """Poll alert conditions and spawn one isolated task for every new incident."""

        while True:
            await asyncio.sleep(self.settings.alert_evaluator_interval_seconds)
            try:
                await self.repository.record_component_heartbeat("alert-evaluator")
                for sandbox in await self.repository.active_sandboxes():
                    identity = self.identities.get(sandbox.scenario_id)
                    if not identity:
                        continue
                    async with self._sandbox_locks[sandbox.id]:
                        created = await self.repository.evaluate_thresholds(
                            sandbox.id, sandbox.scenario_id
                        )
                        for incident_id, run_id in created:
                            incident = await self.repository.incident(incident_id, sandbox.id)
                            if not incident:
                                continue
                            await self.repository.record_run_identity(
                                run_id,
                                incident.scenario_id,
                                identity.subject,
                            )
                            self._spawn(sandbox.id, self._run_agent(incident_id, sandbox.id))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # The next one-second evaluation retries; a transient database failure must not
                # terminate autonomous monitoring.
                continue

    async def _run_agent(self, incident_id: str, sandbox_id: str) -> None:
        """Bound aggregate model load without serialising incidents in one sandbox."""

        async with self._global_agent_limit:
            await self._run_agent_inner(incident_id, sandbox_id)

    async def _run_agent_inner(self, incident_id: str, sandbox_id: str) -> None:
        """Run one traced graph invocation until a durable pause or terminal state."""

        run_started_at = utc_now()
        try:
            graph = getattr(self, "_agent_graph", None) or self._build_agent_graph()
            self._agent_graph = graph
            with mlflow.start_span(
                name="asset_reliability_agent", span_type=SpanType.CHAIN
            ) as span:
                span.set_inputs({"incident_id": incident_id, "sandbox_id": sandbox_id})
                span.set_attribute("asset.incident_id", incident_id)
                mlflow.update_current_trace(session_id=sandbox_id)
                final_state = await graph.ainvoke(
                    {"incident_id": incident_id, "sandbox_id": sandbox_id},
                    config={"recursion_limit": 100},
                )
                if incident := final_state.get("incident"):
                    span.set_attribute("asset.pump_id", incident.asset_id)
                outcome = {
                    "paused": "approval_required",
                    "completed": "work_order_created",
                    "no_action": "no_action",
                }.get(final_state.get("phase"))
                if outcome:
                    span.set_attribute("asset.outcome", outcome)
                span.set_outputs({"phase": final_state.get("phase", "unknown")})
            trace_id = mlflow.get_last_active_trace_id()
            if trace_id:
                await self._record_trace_link(incident_id, trace_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self.repository.append_activity(
                incident_id,
                RuntimeActivity(
                    category="runtime",
                    title="Agent run failed",
                    status="failed",
                    status_label="Failed",
                    summary=str(exc),
                    started_at=run_started_at,
                    completed_at=utc_now(),
                ),
            )
            await self.repository.fail_incident(incident_id, str(exc))
            await self.repository.terminate_run_context(incident_id)
        finally:
            getattr(self, "_agent_identities", {}).pop(incident_id, None)
            getattr(self, "_agent_run_contexts", {}).pop(incident_id, None)

    def _build_agent_graph(self):
        """Compile the explicit investigation graph without a process-local checkpointer."""

        builder = StateGraph(AgentGraphState)
        builder.add_node("load_context", self._load_context_node)
        builder.add_node("load_skill", self._load_skill_node)
        builder.add_node("discover_tools", self._discover_tools_node)
        builder.add_node("invoke_model", self._invoke_model_node)
        builder.add_node("request_governed_action", self._request_governed_action_node)
        builder.add_node("execute_tools", self._execute_tools_node)
        builder.add_node("resolve_no_action", self._resolve_no_action_node)
        builder.add_node("resume_approval", self._resume_approval_node)
        builder.add_conditional_edges(
            START,
            lambda state: state.get("entrypoint", "investigation"),
            {"investigation": "load_context", "approval": "resume_approval"},
        )
        builder.add_conditional_edges(
            "load_context",
            lambda state: "missing" if state.get("phase") == "missing" else "ready",
            {"missing": END, "ready": "load_skill"},
        )
        builder.add_edge("load_skill", "discover_tools")
        builder.add_edge("discover_tools", "invoke_model")
        builder.add_conditional_edges(
            "invoke_model",
            self._route_after_model,
            {
                "tools": "execute_tools",
                "request_action": "request_governed_action",
                "no_action": "resolve_no_action",
            },
        )
        builder.add_edge("request_governed_action", "invoke_model")
        builder.add_conditional_edges(
            "execute_tools",
            lambda state: state["phase"],
            {"continue": "invoke_model", "paused": END, "completed": END},
        )
        builder.add_edge("resolve_no_action", END)
        builder.add_edge("resume_approval", END)
        return builder.compile()

    async def _load_context_node(self, state: AgentGraphState) -> AgentGraphState:
        incident_id = state["incident_id"]
        sandbox_id = state["sandbox_id"]
        incident = await self.repository.incident(incident_id, sandbox_id)
        if not incident:
            return {"phase": "missing"}
        identity = await self._wait_for_identity(incident.scenario_id)
        if not identity:
            raise RuntimeError("No in-memory OBO identity is available for this scenario")
        run_context = await self.repository.create_run_context(incident_id, sandbox_id)
        identities = getattr(self, "_agent_identities", None)
        if identities is None:
            identities = self._agent_identities = {}
        run_contexts = getattr(self, "_agent_run_contexts", None)
        if run_contexts is None:
            run_contexts = self._agent_run_contexts = {}
        identities[incident_id] = identity
        run_contexts[incident_id] = run_context
        return {
            "incident": incident,
            "last_risk": None,
            "action_continuation_requested": False,
            "phase": "ready",
        }

    async def _load_skill_node(self, state: AgentGraphState) -> AgentGraphState:
        if not self.skill_gateway or not self.settings.governed_skill_tool:
            raise RuntimeError("Governed Skill Gateway configuration is missing")
        started_at = utc_now()
        response = await self.skill_gateway.call_tool(
            self.settings.governed_skill_tool, {}, self._graph_identity(state).token
        )
        completed_at = utc_now()
        skill_data = _structured(response)
        skill_instructions = str(skill_data.get("result") or json.dumps(skill_data))
        await self.repository.append_activity(
            state["incident_id"],
            RuntimeActivity(
                category="skill",
                title=self.settings.governed_skill_tool,
                actor=self.settings.governed_skill_tool,
                status="complete",
                status_label="Loaded",
                summary="Loaded the governed response procedure",
                action=(
                    RuntimeAction(
                        label="Read Standard Operating Procedure",
                        kind="link",
                        href=getattr(self.settings, "governed_skill_document_url", None),
                    )
                    if getattr(self.settings, "governed_skill_document_url", None)
                    else None
                ),
                started_at=started_at,
                completed_at=completed_at,
            ),
        )
        incident = state["incident"]
        return {
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a LangGraph asset reliability agent. Follow the governed Skill "
                        "below. Choose tools from the capabilities discovered at runtime; do not "
                        "assume a fixed sequence or call count. Never claim an action ran when a "
                        "tool was blocked. OUTPUT PROTOCOL IS STRICT: whenever you select tools, "
                        "assistant content must be prospective and exactly `Decision rationale: "
                        "I will call [tool name] because [evidence-based reason].` For multiple "
                        "calls, name them in the same prospective sentence. Never claim a call "
                        "ran, returned, completed, or produced evidence before it executes. A "
                        "terminal response must contain exactly four Markdown bullets, in this "
                        "exact order and with no other prose: `- **Finding:** ...`, "
                        "`- **Evidence:** ...`, `- **Risk:** ...`, `- **Action:** ...`.\n\n"
                        f"GOVERNED SKILL:\n{skill_instructions}"
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Autonomous alert for {incident.asset_id}: {incident.condition}. "
                        "Assess failure risk before the next maintenance window and pursue the "
                        "governed response warranted by the evidence. The incident id is "
                        f"{state['incident_id']}."
                    ),
                },
            ]
        }

    async def _discover_tools_node(self, state: AgentGraphState) -> AgentGraphState:
        started_at = utc_now()
        tools = await self.gateway.list_tools(self._graph_identity(state).token)
        completed_at = utc_now()
        available = {tool["name"]: tool for tool in tools}
        await self.repository.append_activity(
            state["incident_id"],
            RuntimeActivity(
                category="mcp",
                title="Discover governed maintenance capabilities",
                actor=MCP_ACTIVITY_ACTOR,
                status="complete",
                status_label="Connected",
                summary=f"Unity Gateway exposed {len(available)} governed tools at runtime.",
                details=[ActivityDatum(label="Available", value="\n".join(sorted(available)))],
                started_at=started_at,
                completed_at=completed_at,
            ),
        )
        return {
            "available": available,
            "openai_tools": [
                {
                    "type": "function",
                    "function": {
                        "name": tool["name"],
                        "description": tool.get("description", ""),
                        "parameters": _model_visible_schema(
                            tool.get("inputSchema", {"type": "object"})
                        ),
                    },
                }
                for tool in tools
            ],
        }

    async def _invoke_model_node(self, state: AgentGraphState) -> AgentGraphState:
        endpoint = self.settings.model_endpoint
        messages = [*state["messages"]]
        started_at = utc_now()
        turn = await self.model.turn(endpoint, messages, tools=state["openai_tools"])
        completed_at = utc_now()
        tool_calls = turn.message.get("tool_calls", [])
        summary = str(turn.message.get("content") or "")
        input_tokens = turn.input_tokens
        output_tokens = turn.output_tokens
        tool_names = [call["function"]["name"] for call in tool_calls]
        canonical_rationale = (
            _canonical_decision_rationale(summary, tool_names) if tool_calls else None
        )
        if tool_calls and not canonical_rationale:
            try:
                rationale = await self.model.turn(
                    endpoint,
                    [
                        {
                            "role": "system",
                            "content": (
                                "Return one prospective sentence exactly beginning "
                                f"`{_rationale_prefix(tool_names)}` followed by the "
                                "evidence-based reason. Do not claim any call already ran, "
                                "returned, completed, or produced a result."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                "Explain the evidence-based reason for the selected governed "
                                f"tool call(s). Selected calls: {json.dumps(tool_calls)}. Recent "
                                f"context: {json.dumps(messages[-3:])}"
                            ),
                        },
                    ],
                )
                summary = str(rationale.message.get("content") or "")
                input_tokens += rationale.input_tokens
                output_tokens += rationale.output_tokens
                completed_at = utc_now()
                canonical_rationale = _canonical_decision_rationale(summary, tool_names)
            except Exception:  # Rationale formatting must never fail the agent run.
                canonical_rationale = None
            if not canonical_rationale:
                canonical_rationale = _fallback_decision_rationale(tool_names)
        if canonical_rationale:
            summary = canonical_rationale
        last_risk = state.get("last_risk")
        canonical_terminal = (
            _canonical_terminal_response(summary)
            if not tool_calls and last_risk and last_risk.get("risk_band") != "high"
            else None
        )
        await self.repository.append_activity(
            state["incident_id"],
            RuntimeActivity(
                category="model_call",
                title="Model execution",
                actor=endpoint,
                status="complete",
                status_label="Completed",
                summary=canonical_terminal or summary,
                details=[
                    ActivityDatum(label="Input tokens", value=str(input_tokens)),
                    ActivityDatum(label="Output tokens", value=str(output_tokens)),
                    ActivityDatum(
                        label="Purpose",
                        value=(
                            "Agent reasoning and tool selection"
                            if tool_calls
                            else "Final incident response"
                        ),
                    ),
                    ActivityDatum(
                        label="Estimated cost",
                        value=f"{self._model_cost(input_tokens, output_tokens):.8f}",
                    ),
                ]
                + (
                    [ActivityDatum(label="Model output", value=summary)] if tool_calls else []
                ),
                started_at=started_at,
                completed_at=completed_at,
            ),
            tokens=(
                input_tokens,
                output_tokens,
                self._model_cost(input_tokens, output_tokens),
            ),
        )
        messages.append(turn.message)
        return {
            "messages": messages,
            "tool_calls": tool_calls,
            "model_summary": summary,
            "terminal_response": canonical_terminal,
        }

    @staticmethod
    def _route_after_model(state: AgentGraphState) -> str:
        if state["tool_calls"]:
            return "tools"
        last_risk = state.get("last_risk")
        if (
            last_risk
            and last_risk.get("risk_band") == "high"
            and not state.get("action_continuation_requested", False)
        ):
            return "request_action"
        if last_risk and last_risk.get("risk_band") != "high":
            return "no_action"
        raise RuntimeError("Agent ended without resolving the incident through governed evidence")

    async def _request_governed_action_node(
        self, state: AgentGraphState
    ) -> AgentGraphState:
        return {
            "messages": [
                *state["messages"],
                {
                    "role": "user",
                    "content": (
                        "High risk is established, but no governed action has been attempted. "
                        "Continue the Skill workflow by invoking the available governed action "
                        "warranted by the evidence. Do not predict the policy decision; let "
                        "Unity Gateway decide."
                    ),
                },
            ],
            "action_continuation_requested": True,
        }

    async def _execute_tools_node(self, state: AgentGraphState) -> AgentGraphState:
        messages = [*state["messages"]]
        last_risk = state.get("last_risk")
        for tool_call in state["tool_calls"]:
            function = tool_call["function"]
            tool_name = function["name"]
            if tool_name not in state["available"]:
                raise RuntimeError(f"Model selected unavailable tool: {tool_name}")
            raw_arguments = function.get("arguments") or "{}"
            arguments = (
                json.loads(raw_arguments) if isinstance(raw_arguments, str) else raw_arguments
            )
            if tool_name == "submit_work_order":
                arguments = _submit_work_order_arguments(
                    arguments, state["incident"], last_risk
                )
            started_at = utc_now()
            try:
                result = await self.gateway.call_tool(
                    tool_name,
                    {
                        **arguments,
                        INTERNAL_RUN_CONTEXT: self._graph_run_context(state),
                    },
                    self._graph_identity(state).token,
                )
            except GatewayAsk as ask:
                await self._pause_for_gateway(
                    state["incident_id"],
                    tool_name,
                    ask,
                    started_at=started_at,
                    incident=state["incident"],
                    arguments=arguments,
                    risk=last_risk,
                )
                return {"messages": messages, "last_risk": last_risk, "phase": "paused"}
            completed_at = utc_now()
            data = _structured(result)
            await self.repository.append_activity(
                state["incident_id"],
                RuntimeActivity(
                    category="tool_call",
                    title=tool_name,
                    actor=f"Unity Gateway · {tool_name}",
                    status="complete",
                    status_label="ALLOW",
                    summary=_result_summary(data),
                    details=[
                        ActivityDatum(label="Decision", value="ALLOW"),
                        ActivityDatum(label="Execution", value="Executed"),
                    ],
                    started_at=started_at,
                    completed_at=completed_at,
                ),
                tool_call=True,
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "name": tool_name,
                    "content": json.dumps(data),
                }
            )
            if tool_name == "assess_failure_risk":
                last_risk = data
            if tool_name == "submit_work_order":
                await self._complete_governed_action(
                    state["incident_id"],
                    result,
                    started_at=started_at,
                    completed_at=completed_at,
                )
                return {"messages": messages, "last_risk": last_risk, "phase": "completed"}
        return {"messages": messages, "last_risk": last_risk, "phase": "continue"}

    def _graph_identity(self, state: AgentGraphState) -> OboIdentity:
        """Resolve an OBO identity held outside traceable graph state."""

        identity = getattr(self, "_agent_identities", {}).get(state["incident_id"])
        if not identity:
            raise RuntimeError("The in-memory OBO identity expired during agent execution")
        return identity

    def _graph_run_context(self, state: AgentGraphState) -> str:
        """Resolve an opaque Gateway capability held outside traceable graph state."""

        run_context = getattr(self, "_agent_run_contexts", {}).get(state["incident_id"])
        if not run_context:
            raise RuntimeError("The in-memory Gateway run context expired during agent execution")
        return run_context

    async def _resolve_no_action_node(self, state: AgentGraphState) -> AgentGraphState:
        if not state.get("terminal_response"):
            await self._ensure_terminal_response(
                state["incident_id"],
                self.settings.model_endpoint,
                state["messages"],
                state["model_summary"],
            )
        await self.repository.resolve_no_action(state["incident_id"])
        await self.repository.terminate_run_context(state["incident_id"])
        return {"phase": "no_action"}

    async def _resume_approval_node(self, state: AgentGraphState) -> AgentGraphState:
        """Resume from Lakebase and replay only the exact claimed Gateway request."""

        completed = await self._retry_pending(
            state["incident_id"],
            state["pending_approval"],
            state["approving_subject"],
            state["sandbox_id"],
        )
        return {
            "replay_completed": completed,
            "phase": "completed" if completed else "paused",
        }

    async def _record_trace_link(self, incident_id: str, trace_id: str) -> None:
        """Expose a real trace link when host and experiment identity are configured."""

        experiment_id = getattr(self.settings, "mlflow_experiment_id", None)
        host = str(getattr(self.settings, "databricks_host", "") or "").rstrip("/")
        if not experiment_id or not host:
            return
        browser_trace_id = trace_id.rsplit("/", 1)[-1]
        now = utc_now()
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="trace",
                title="MLflow trace",
                status="complete",
                status_label="Recorded",
                summary="LangGraph execution was recorded in MLflow.",
                action=RuntimeAction(
                    label="Open MLflow trace",
                    kind="link",
                    href=f"{host}/ml/experiments/{experiment_id}/traces/{browser_trace_id}",
                ),
                started_at=now,
                completed_at=now,
            ),
        )

    async def _wait_for_identity(self, scenario_id: str) -> OboIdentity | None:
        """Allow a local hot reload's next dashboard poll to restore process-local OBO state."""
        for _ in range(60):
            identity = self.identities.get(scenario_id)
            if identity:
                return identity
            await asyncio.sleep(0.5)
        return None

    async def _pause_for_gateway(
        self,
        incident_id: str,
        tool_name: str,
        ask: GatewayAsk,
        *,
        started_at: datetime,
        incident: IncidentDetail | None = None,
        arguments: dict[str, Any] | None = None,
        risk: dict[str, Any] | None = None,
    ) -> None:
        """Persist Gateway ``ASK`` as a pure pause without executing the source tool."""

        pending = PendingApproval(
            serialized_request=ask.serialized_request,
            elicitation_id=ask.elicitation.elicitation_id,
            approval_url=ask.elicitation.url,
            message=ask.elicitation.message,
            tool_name=tool_name,
        )
        details = [
            ActivityDatum(label="Decision", value="ASK"),
            ActivityDatum(label="Execution", value="Not executed"),
        ]
        if tool_name == "submit_work_order" and incident and arguments:
            probability = risk.get("failure_probability_pct") if risk else None
            risk_band = risk.get("risk_band") if risk else None
            risk_text = (
                f"{probability}% probability of failure before the maintenance window"
                + (f" ({risk_band} risk)." if risk_band else ".")
                if probability is not None
                else "The governed assessment requires immediate intervention."
            )
            details.extend(
                [
                    ActivityDatum(label="Finding", value=incident.condition.rstrip(".") + "."),
                    ActivityDatum(
                        label="Evidence",
                        value=str(
                            (risk or {}).get(
                                "evidence", "The runtime assessment identified persistent risk."
                            )
                        ).rstrip(".")
                        + ".",
                    ),
                    ActivityDatum(label="Risk", value=risk_text),
                    ActivityDatum(
                        label="Action",
                        value=(
                            f"{arguments['action'].rstrip('.')} during "
                            f"{arguments['planned_window']}."
                        ),
                    ),
                    ActivityDatum(label="Rationale", value=arguments["rationale"]),
                    ActivityDatum(
                        label="Planned window", value=arguments["planned_window"]
                    ),
                ]
            )
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="tool_call",
                title=tool_name,
                actor=f"Unity Gateway · {tool_name}",
                status="waiting",
                status_label="ASK · Not executed",
                summary=(
                    f"Approval required to call tool {tool_name} on {MCP_ACTIVITY_ACTOR}."
                ),
                details=details,
                action=RuntimeAction(
                    label="Review approval request",
                    kind="link",
                    href=ask.elicitation.url,
                    endpoint=f"/api/incidents/{incident_id}/approve",
                ),
                started_at=started_at,
                completed_at=None,
            ),
            tool_call=True,
        )
        await self.repository.set_waiting(incident_id, pending)

    def _model_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (
            input_tokens * self.settings.model_input_cost_per_million
            + output_tokens * self.settings.model_output_cost_per_million
        ) / 1_000_000

    async def _retry_pending(
        self,
        incident_id: str,
        pending: PendingApproval,
        approving_subject: str,
        sandbox_id: str,
    ) -> bool:
        """Single-flight replay of the exact paused request after Gateway authorization."""

        incident = await self.repository.incident(incident_id, sandbox_id)
        if not incident or incident.status != "waiting":
            return True
        identity = self.identities.get(incident.scenario_id)
        if not identity:
            return False
        claimed = await self.repository.claim_pending_approval(incident_id, approving_subject)
        if not claimed:
            latest = await self.repository.incident(incident_id, sandbox_id)
            return bool(latest and latest.status == "completed")
        pending = claimed.pending
        try:
            replay_started_at = utc_now()
            result = await self.gateway.replay(pending.serialized_request, identity.token)
        except GatewayAsk:
            await self.repository.release_pending_approval(
                incident_id, claimed.claim_id, claimed.version
            )
            return False
        except Exception:
            await self.repository.release_pending_approval(
                incident_id, claimed.claim_id, claimed.version
            )
            raise
        replay_completed_at = utc_now()
        data = _structured(result)
        work_order_id = str(data.get("work_order_id", "Work order"))
        receipt = {
            "elicitation_id": pending.elicitation_id,
            "request_sha256": hashlib.sha256(
                pending.serialized_request.encode("utf-8")
            ).hexdigest(),
            "approving_subject": approving_subject,
            "decision": "ALLOW",
            "requested_at": pending.created_at.isoformat(),
            "decided_at": replay_started_at.isoformat(),
            "completed_at": replay_completed_at.isoformat(),
        }
        finalized = await self.repository.record_governance_receipt(
            incident_id,
            work_order_id,
            receipt,
            claimed.claim_id,
            claimed.version,
        )
        if not finalized:
            return False
        await self._complete_governed_action(
            incident_id,
            result,
            started_at=replay_started_at,
            completed_at=replay_completed_at,
            persist_completion=False,
        )
        await self._generate_post_approval_response(incident_id, incident, result)
        return True

    async def _complete_governed_action(
        self,
        incident_id: str,
        result: dict[str, Any],
        *,
        started_at: datetime,
        completed_at: datetime,
        persist_completion: bool = True,
    ) -> None:
        data = _structured(result)
        work_order_id = data.get("work_order_id", "Work order")
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="tool_result",
                title="submit_work_order",
                actor="Unity Gateway · submit_work_order",
                status="complete",
                status_label="APPROVED · ALLOW",
                summary=f"{work_order_id} scheduled for immediate maintenance.",
                details=[
                    ActivityDatum(label="Work order", value=str(work_order_id)),
                    ActivityDatum(label="Decision", value="ALLOW"),
                    ActivityDatum(label="Execution", value="Executed"),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
        )
        if persist_completion:
            await self.repository.complete_incident(incident_id, str(work_order_id))
        await self.repository.terminate_run_context(incident_id)

    async def _generate_post_approval_response(
        self, incident_id: str, incident: IncidentDetail, result: dict[str, Any]
    ) -> None:
        started_at = utc_now()
        messages = [
            {
                "role": "system",
                "content": (
                    "Return exactly four Markdown bullets in this order and no other prose: "
                    "`- **Finding:** ...`, `- **Evidence:** ...`, `- **Risk:** ...`, "
                    "`- **Action:** ...`. The governed action has now executed successfully."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Incident: {incident.model_dump_json()}. Governed result: "
                    f"{json.dumps(_structured(result))}"
                ),
            },
        ]
        try:
            turn = await self.model.turn(self.settings.model_endpoint, messages)
        except Exception:  # The executed governed write must still receive a conclusion.
            await self._append_fallback_response(
                incident_id, _fallback_terminal_response(incident, result)
            )
            return
        completed_at = utc_now()
        content = str(turn.message.get("content") or "")
        canonical = _canonical_terminal_response(content)
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="model_call",
                title="Model execution",
                actor=self.settings.model_endpoint,
                status="complete",
                status_label="Completed",
                summary=canonical or content,
                details=[
                    ActivityDatum(label="Input tokens", value=str(turn.input_tokens)),
                    ActivityDatum(label="Output tokens", value=str(turn.output_tokens)),
                    ActivityDatum(label="Purpose", value="Final incident response"),
                    ActivityDatum(
                        label="Estimated cost",
                        value=f"{self._model_cost(turn.input_tokens, turn.output_tokens):.8f}",
                    ),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
            tokens=(
                turn.input_tokens,
                turn.output_tokens,
                self._model_cost(turn.input_tokens, turn.output_tokens),
            ),
        )
        try:
            canonical or await self._ensure_terminal_response(
                incident_id,
                self.settings.model_endpoint,
                [*messages, turn.message],
                content,
            )
        except Exception:
            await self._append_fallback_response(
                incident_id, _fallback_terminal_response(incident, result)
            )

    async def _append_fallback_response(self, incident_id: str, content: str) -> None:
        now = utc_now()
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="runtime",
                title="Deterministic response",
                status="complete",
                status_label="Completed",
                summary=content,
                started_at=now,
                completed_at=now,
            ),
        )

    async def _ensure_terminal_response(
        self,
        incident_id: str,
        endpoint: str,
        messages: list[dict[str, Any]],
        content: str,
    ) -> str:
        canonical = _canonical_terminal_response(content)
        if canonical:
            return canonical
        correction_messages = [
            *messages,
            {
                "role": "user",
                "content": (
                    "Reformat the conclusion only. Return exactly four Markdown bullets in "
                    "this order, with no heading or other prose: `- **Finding:** ...`, "
                    "`- **Evidence:** ...`, `- **Risk:** ...`, `- **Action:** ...`."
                ),
            },
        ]
        started_at = utc_now()
        corrected = await self.model.turn(endpoint, correction_messages)
        completed_at = utc_now()
        corrected_content = str(corrected.message.get("content") or "")
        canonical = _canonical_terminal_response(corrected_content)
        corrected_cost = self._model_cost(corrected.input_tokens, corrected.output_tokens)
        await self.repository.append_activity(
            incident_id,
            RuntimeActivity(
                category="model_call",
                title="Model execution",
                actor=endpoint,
                status="complete",
                status_label="Completed",
                summary=canonical or corrected_content,
                details=[
                    ActivityDatum(label="Input tokens", value=str(corrected.input_tokens)),
                    ActivityDatum(label="Output tokens", value=str(corrected.output_tokens)),
                    ActivityDatum(label="Purpose", value="Response format correction"),
                    ActivityDatum(
                        label="Estimated cost",
                        value=f"{corrected_cost:.8f}",
                    ),
                ],
                started_at=started_at,
                completed_at=completed_at,
            ),
            tokens=(
                corrected.input_tokens,
                corrected.output_tokens,
                corrected_cost,
            ),
        )
        if not canonical:
            raise RuntimeError(
                "Model terminal output violated the response protocol after reformat"
            )
        return canonical


def _model_visible_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Remove the internal sandbox capability before exposing tools to the model."""

    visible = json.loads(json.dumps(schema))
    visible.get("properties", {}).pop(INTERNAL_RUN_CONTEXT, None)
    visible["required"] = [
        item for item in visible.get("required", []) if item != INTERNAL_RUN_CONTEXT
    ]
    return visible


def _structured(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result", {})
    if isinstance(result.get("structuredContent"), dict):
        return result["structuredContent"]
    for item in result.get("content", []):
        if item.get("type") == "text":
            try:
                value = json.loads(item["text"])
                if isinstance(value, dict):
                    return value
            except json.JSONDecodeError:
                return {"result": item["text"]}
    return result if isinstance(result, dict) else {"result": result}


def _submit_work_order_arguments(
    arguments: dict[str, Any], incident: IncidentDetail, risk: dict[str, Any] | None
) -> dict[str, Any]:
    """Enforce the governed write contract and add a concise evidence-based rationale."""

    required = ("asset_id", "action", "incident_id")
    missing = [name for name in required if not str(arguments.get(name, "")).strip()]
    if missing:
        raise RuntimeError(
            "submit_work_order omitted required arguments: " + ", ".join(missing)
        )
    normalized = dict(arguments)
    normalized["planned_window"] = "immediate maintenance"
    rationale = " ".join(str(normalized.get("rationale") or "").split())
    if not rationale:
        evidence = str((risk or {}).get("evidence") or incident.condition).rstrip(".")
        probability = (risk or {}).get("failure_probability_pct")
        risk_clause = (
            f" with an assessed failure probability of {probability}%"
            if probability
            else ""
        )
        rationale = (
            f"{str(normalized['action']).rstrip('.')} for {incident.asset_id} because "
            f"{evidence.lower()}{risk_clause}, requiring immediate maintenance."
        )
    normalized["rationale"] = rationale
    return normalized


def _result_summary(data: dict[str, Any]) -> str:
    if "failure_probability_pct" in data:
        return f"Failure risk assessed at {data['failure_probability_pct']}% ({data['risk_band']})."
    if "vibration_mm_s" in data:
        context = [f"current vibration of {_display_number(data['vibration_mm_s'])} mm/s"]
        if "warning_threshold_mm_s" in data:
            context[0] += (
                " against the "
                f"{_display_number(data['warning_threshold_mm_s'])} mm/s warning threshold"
            )
        if "flow_m3_h" in data:
            context.append(f"flow of {_display_number(data['flow_m3_h'])} m³/h")
        if "current_a" in data:
            context.append(f"motor current of {_display_number(data['current_a'])} A")
        trend = [value for value in data.get("trend", []) if isinstance(value, int | float)]
        if trend:
            context.append(
                f"{len(trend)} recent vibration readings spanning "
                f"{_display_number(min(trend))}–{_display_number(max(trend))} mm/s"
            )
        return f"The MCP server returned {_natural_join(context)}."
    if data.get("part_available"):
        return (
            f"{data.get('part')} is available; estimated duration is "
            f"{data.get('estimated_duration_hours')} h."
        )
    return "Governed tool completed successfully."


def _display_number(value: Any) -> str:
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def _natural_join(values: list[str]) -> str:
    if len(values) == 1:
        return values[0]
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def _fallback_terminal_response(incident: IncidentDetail, result: dict[str, Any]) -> str:
    data = _structured(result)
    work_order_id = str(data.get("work_order_id", "the approved work order"))
    return (
        f"- **Finding:** {incident.condition.rstrip('.')} requires maintenance.\n"
        f"- **Evidence:** The governed assessment confirmed the issue on {incident.asset_id}.\n"
        "- **Risk:** The evidence warrants action before the next maintenance window.\n"
        f"- **Action:** Work order {work_order_id} was approved and scheduled."
    )


def _rationale_prefix(tool_names: list[str]) -> str:
    if not tool_names:
        return "Decision rationale: I will call the selected tool because "
    calls = (
        tool_names[0]
        if len(tool_names) == 1
        else f"{', '.join(tool_names[:-1])} and {tool_names[-1]}"
    )
    return f"Decision rationale: I will call {calls} because "


def _fallback_decision_rationale(tool_names: list[str]) -> str:
    reason = (
        "the current incident evidence requires this governed capability"
        if len(tool_names) == 1
        else "the current incident evidence requires these governed capabilities"
    )
    rationale = f"{_rationale_prefix(tool_names)}{reason}."
    if not _valid_decision_rationale(rationale, tool_names):
        raise AssertionError("Deterministic rationale fallback violated its own format")
    return rationale


def _valid_decision_rationale(content: str, tool_names: list[str]) -> bool:
    prefix = _rationale_prefix(tool_names)
    if not content.startswith(prefix) or "\n" in content:
        return False
    rationale = content.removeprefix(prefix).strip()
    retrospective = re.compile(
        r"\b(called|ran|returned|completed|executed|received|loaded|found|produced)\b",
        flags=re.I,
    )
    return (
        bool(rationale)
        and not retrospective.search(rationale)
        and sum(rationale.count(mark) for mark in ".!?") == 1
        and rationale.endswith(".")
    )


def _canonical_decision_rationale(content: str, tool_names: list[str]) -> str | None:
    """Preserve model wording while enforcing the presentation-safe rationale envelope."""
    cleaned = re.sub(r"^```(?:markdown|text)?\s*|\s*```$", "", content.strip(), flags=re.I)
    if not cleaned:
        return None
    match = re.search(r"\bbecause\s+", cleaned, flags=re.I)
    if match:
        rationale = cleaned[match.end() :]
    else:
        label = re.search(r"decision\s+rationale\s*:\s*", cleaned, flags=re.I)
        rationale = cleaned[label.end() :] if label else cleaned
    rationale = re.sub(r"^[\s#>*-]+", "", rationale)
    rationale = re.sub(r"\*\*|__|`", "", rationale)
    rationale = " ".join(rationale.split())
    if not rationale:
        return None
    retrospective = re.compile(
        r"\b(called|ran|returned|completed|executed|received|loaded|found|produced)\b",
        flags=re.I,
    )
    if retrospective.search(rationale):
        rationale = "the available incident evidence warrants this governed action."
    concise = re.split(r"(?<=[.!?])\s+", rationale)[0].strip()
    if not concise:
        return None
    concise = concise.rstrip("!?")
    if not concise.endswith("."):
        concise += "."
    canonical = f"{_rationale_prefix(tool_names)}{concise[0].lower()}{concise[1:]}"
    return canonical if _valid_decision_rationale(canonical, tool_names) else None


def _valid_terminal_response(content: str) -> bool:
    lines = content.splitlines()
    labels = ("Finding", "Evidence", "Risk", "Action")
    return len(lines) == 4 and all(
        line.startswith(f"- **{label}:** ") and bool(line.removeprefix(f"- **{label}:** ").strip())
        for line, label in zip(lines, labels, strict=True)
    )


def _canonical_terminal_response(content: str) -> str | None:
    """Extract the four required fields and render the exact presentation contract."""
    cleaned = re.sub(r"^```(?:markdown|text)?\s*|\s*```$", "", content.strip(), flags=re.I)
    if not cleaned:
        return None
    labels = ("Finding", "Evidence", "Risk", "Action")
    values: list[str] = []
    for index, label in enumerate(labels):
        next_label = labels[index + 1] if index + 1 < len(labels) else None
        boundary = rf"(?=\s*(?:[-*]\s*)?\*{{0,2}}{next_label}:\*{{0,2}})" if next_label else r"$"
        match = re.search(
            rf"(?:[-*]\s*)?\*{{0,2}}{label}:\*{{0,2}}\s*(.*?){boundary}",
            cleaned,
            flags=re.I | re.S,
        )
        if not match:
            return None
        value = " ".join(match.group(1).strip().split())
        if not value:
            return None
        values.append(value)
    return "\n".join(f"- **{label}:** {value}" for label, value in zip(labels, values, strict=True))
