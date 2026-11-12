"""Persistence protocol and deterministic in-memory implementation for local development."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from backend.models import (
    AppUser,
    DashboardSnapshot,
    IncidentDetail,
    SandboxState,
    ScenarioCommand,
    ScenarioState,
    utc_now,
)


class Mutation(Protocol):
    def __call__(
        self, snapshot: DashboardSnapshot, incidents: dict[str, IncidentDetail]
    ) -> None: ...


class CommandCenterRepository(Protocol):
    """Operations required by both local and Lakebase command-center runtimes."""

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def resolve_sandbox(self, user: AppUser) -> SandboxState: ...
    async def active_sandboxes(self) -> list[SandboxState]: ...
    async def snapshot(self, sandbox_id: str | None = None) -> DashboardSnapshot: ...
    async def incident(
        self, incident_id: str, sandbox_id: str | None = None
    ) -> IncidentDetail | None: ...
    async def mutate(self, operation: Mutation) -> DashboardSnapshot: ...
    async def wait_for_change(
        self, after_revision: int, timeout: float, sandbox_id: str | None = None
    ) -> DashboardSnapshot: ...
    async def enqueue_command(self, command: ScenarioCommand) -> None: ...
    async def claim_commands(self, limit: int = 10) -> list[ScenarioCommand]: ...
    async def finish_command(self, command_id: str, error: str | None = None) -> None: ...
    async def record_run_identity(
        self, run_id: str, scenario_id: str, subject: str | None
    ) -> None: ...
    async def create_work_order(
        self, work_order_id: str, incident_id: str, payload: dict[str, Any],
        sandbox_id: str | None = None,
    ) -> dict[str, Any]: ...
    async def telemetry_window(
        self, asset_id: str, limit: int = 12, sandbox_id: str | None = None
    ) -> list[dict[str, Any]]: ...
    async def register_user(self, user: AppUser) -> SandboxState: ...
    async def resolve_run_context(self, raw_context: str) -> dict[str, str] | None: ...


class LocalCommandCenterRepository:
    """Deterministic in-memory adapter implementing the production persistence contract."""

    def __init__(self, initial: DashboardSnapshot) -> None:
        self._snapshot = initial
        self._incidents: dict[str, IncidentDetail] = {}
        self._commands: dict[str, ScenarioCommand] = {}
        self._identities: dict[str, dict[str, Any]] = {}
        self._work_orders: dict[str, dict[str, Any]] = {}
        self._operator_audit: list[dict[str, Any]] = []
        self._condition = asyncio.Condition()
        sandbox = SandboxState(
            id="local-sandbox",
            scenario_id=initial.scenario.id,
            generator_slot=0,
            owner_email="local.operator@databricks.com",
        )
        initial.sandbox = sandbox
        self._sandboxes: dict[str, SandboxState] = {sandbox.id: sandbox}
        self._snapshots: dict[str, DashboardSnapshot] = {sandbox.id: initial}

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def resolve_sandbox(self, user: AppUser) -> SandboxState:
        sandbox = next(
            (item for item in self._sandboxes.values() if item.owner_email == user.email), None
        )
        if sandbox:
            return sandbox.model_copy(deep=True)
        sandbox = SandboxState(
            id=f"local-{len(self._sandboxes)}",
            scenario_id=ScenarioState().id,
            generator_slot=len(self._sandboxes),
            owner_email=user.email,
        )
        self._sandboxes[sandbox.id] = sandbox
        personal = self._snapshot.model_copy(deep=True)
        personal.scenario = ScenarioState(id=sandbox.scenario_id)
        personal.sandbox = sandbox.model_copy(deep=True)
        personal.alerts = []
        personal.invocations = []
        personal.active_runs = []
        self._snapshots[sandbox.id] = personal
        return sandbox.model_copy(deep=True)

    async def active_sandboxes(self) -> list[SandboxState]:
        return [item.model_copy(deep=True) for item in self._sandboxes.values()]

    async def snapshot(self, sandbox_id: str | None = None) -> DashboardSnapshot:
        async with self._condition:
            source = self._snapshots.get(sandbox_id or "local-sandbox", self._snapshot)
            snapshot = source.model_copy(deep=True)
            if sandbox_id:
                sandbox = self._sandboxes.get(sandbox_id)
                if not sandbox:
                    raise LookupError("Personal sandbox not found")
                snapshot.sandbox = sandbox.model_copy(deep=True)
            return snapshot

    async def incident(
        self, incident_id: str, sandbox_id: str | None = None
    ) -> IncidentDetail | None:
        async with self._condition:
            incident = self._incidents.get(incident_id)
            if incident and sandbox_id and incident.sandbox_id != sandbox_id:
                return None
            return incident.model_copy(deep=True) if incident else None

    async def mutate(self, operation: Mutation) -> DashboardSnapshot:
        return await self.mutate_sandbox("local-sandbox", operation)

    async def mutate_sandbox(
        self, sandbox_id: str, operation: Mutation
    ) -> DashboardSnapshot:
        async with self._condition:
            snapshot = self._snapshots[sandbox_id]
            operation(snapshot, self._incidents)
            snapshot.revision += 1
            if snapshot.sandbox:
                self._sandboxes[sandbox_id] = snapshot.sandbox.model_copy(deep=True)
            if sandbox_id == "local-sandbox":
                self._snapshot = snapshot
            self._condition.notify_all()
            return snapshot.model_copy(deep=True)

    async def wait_for_change(
        self, after_revision: int, timeout: float, sandbox_id: str | None = None
    ) -> DashboardSnapshot:
        async with self._condition:
            source = self._snapshots.get(sandbox_id or "local-sandbox", self._snapshot)
            if source.revision <= after_revision:
                try:
                    await asyncio.wait_for(
                        self._condition.wait_for(lambda: source.revision > after_revision),
                        timeout=timeout,
                    )
                except TimeoutError:
                    pass
            snapshot = source.model_copy(deep=True)
            if sandbox_id:
                snapshot.sandbox = self._sandboxes[sandbox_id].model_copy(deep=True)
            return snapshot

    async def register_user(self, user: AppUser) -> SandboxState:
        return await self.resolve_sandbox(user)

    async def enqueue_command(self, command: ScenarioCommand) -> None:
        async with self._condition:
            duplicate = any(
                item.scenario_id == command.scenario_id
                and item.kind == command.kind
                and item.status != "failed"
                for item in self._commands.values()
            )
            if duplicate:
                raise ValueError("P-204 excursion already queued for this scenario")
            self._commands[command.id] = command.model_copy(deep=True)

    async def claim_commands(self, limit: int = 10) -> list[ScenarioCommand]:
        async with self._condition:
            claimed: list[ScenarioCommand] = []
            for command in self._commands.values():
                if command.status != "pending":
                    continue
                command.status = "running"
                claimed.append(command.model_copy(deep=True))
                if len(claimed) >= limit:
                    break
            return claimed

    async def finish_command(self, command_id: str, error: str | None = None) -> None:
        async with self._condition:
            command = self._commands[command_id]
            command.status = "failed" if error else "completed"
            command.error = error
            command.completed_at = utc_now()

    async def record_run_identity(self, run_id: str, scenario_id: str, subject: str | None) -> None:
        async with self._condition:
            self._identities[run_id] = {
                "scenario_id": scenario_id,
                "subject": subject,
                "captured_at": utc_now(),
            }

    async def create_work_order(
        self,
        work_order_id: str,
        incident_id: str,
        payload: dict[str, Any],
        sandbox_id: str | None = None,
    ) -> dict[str, Any]:
        async with self._condition:
            incident = self._incidents.get(incident_id)
            if sandbox_id and (not incident or incident.sandbox_id != sandbox_id):
                raise ValueError("Cross-sandbox work order rejected")
            existing = self._work_orders.get(work_order_id)
            if existing:
                return dict(existing)
            record = {
                "work_order_id": work_order_id,
                "incident_id": incident_id,
                "status": "scheduled",
                **payload,
                "created_at": utc_now().isoformat(),
            }
            self._work_orders[work_order_id] = record
            return dict(record)

    async def telemetry_window(
        self, asset_id: str, limit: int = 12, sandbox_id: str | None = None
    ) -> list[dict[str, Any]]:
        async with self._condition:
            pump = next(item for item in self._snapshot.pumps if item.asset_id == asset_id)
            values = pump.trend[-limit:]
            return [
                {
                    "vibration_mm_s": value,
                    "flow_m3h": pump.flow_m3_h,
                    "motor_current_a": pump.current_a,
                    "bearing_temperature_c": 59.0 + max(0, value - 4.4) * 3,
                }
                for value in values
            ]

    async def resolve_run_context(self, raw_context: str) -> dict[str, str] | None:
        return self._identities.get(raw_context)
