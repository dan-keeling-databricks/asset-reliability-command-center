"""Validate and dependency-order deployment of the personal-sandbox data plane.

The helper first grants the existing App service principal the minimum workload privileges and
runs the idempotent Lakebase migration as that schema-owning identity. It then deploys the full
bundle, stops the obsolete Kafka producer and governed-history pipeline runner, and starts only
the shared Spark Rate Real-Time Mode job. This ordering keeps the old writer compatible until the
additive migration is complete and prevents paused reference workloads from continuing to run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACCESS_GROUP = "users"
DEFAULT_TIMEOUT_SECONDS = 1_200
COMPUTE_POLICY_KEY = "asset_reliability_workloads"
LAKEBASE_MIGRATION_KEY = "migrate_lakebase_schema"
RATE_JOB_KEY = "telemetry_rtm"
OBSOLETE_CONTINUOUS_JOB_KEYS = (
    "telemetry_producer",
    "governed_telemetry_history_continuous",
)


def run(
    command: list[str], *, capture: bool = False, check: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run one command from the bundle root without invoking a shell."""

    return subprocess.run(  # noqa: S603
        command,
        cwd=REPOSITORY_ROOT,
        check=check,
        text=True,
        capture_output=capture,
    )


def run_json(command: list[str]) -> dict[str, Any]:
    """Run a checked CLI command and decode its JSON object response."""

    result = run(command, capture=True)
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object from {' '.join(command)}")
    return payload


def bundle_command(action: str, args: argparse.Namespace, *extra: str) -> list[str]:
    """Build a bundle command with an explicit profile, target, and access group."""

    return [
        "databricks",
        "bundle",
        action,
        "--target",
        args.target,
        "--profile",
        args.profile,
        "--var",
        f"app_access_group={args.access_group}",
        "--var",
        f"app_service_principal_client_id={args.app_service_principal_client_id}",
        "--var",
        f"catalog={args.catalog}",
        "--var",
        f"schema={args.schema}",
        *extra,
    ]


def workspace_command(args: argparse.Namespace, *parts: str) -> list[str]:
    """Build a non-bundle Databricks CLI command with an explicit profile."""

    return ["databricks", *parts, "--profile", args.profile]


def bundle_summary(args: argparse.Namespace) -> dict[str, Any]:
    """Return remote-aware bundle output used to resolve generated resource IDs."""

    return run_json(bundle_command("summary", args, "--force-pull", "--output", "json"))


def resource_id(args: argparse.Namespace, kind: str, key: str) -> str:
    """Resolve a deployed bundle resource ID and fail clearly if deployment was incomplete."""

    resource = bundle_summary(args).get("resources", {}).get(kind, {}).get(key, {})
    identifier = resource.get("id")
    if not identifier:
        raise RuntimeError(f"Bundle resource {kind}.{key} has no deployed ID")
    return str(identifier)


def bundle_source_directory(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve the synced bundle directory and its workspace object ID.

    Bundle resource definitions point jobs and pipelines at notebooks below
    ``workspace.file_path``.  A workload running as the App service principal therefore
    needs explicit read access to that directory; access inherited by the human deployer is
    not sufficient.  Resolving the path from bundle metadata keeps this helper portable
    across users, targets, and workspaces.
    """

    summary = bundle_summary(args)
    source_path = summary.get("workspace", {}).get("file_path")
    if not source_path:
        raise RuntimeError("Bundle summary does not expose workspace.file_path")
    status = run_json(
        workspace_command(
            args,
            "workspace",
            "get-status",
            str(source_path),
            "--output",
            "json",
        )
    )
    object_id = status.get("object_id")
    if not object_id:
        raise RuntimeError(f"Workspace source directory {source_path} has no object ID")
    return str(source_path), str(object_id)


def grant_bundle_source_read(args: argparse.Namespace) -> None:
    """Idempotently grant the workload identity read access to synced bundle sources."""

    source_path, object_id = bundle_source_directory(args)
    access_control = {
        "access_control_list": [
            {
                "service_principal_name": args.app_service_principal_client_id,
                "permission_level": "CAN_READ",
            }
        ]
    }
    run_json(
        workspace_command(
            args,
            "workspace",
            "update-permissions",
            "directories",
            object_id,
            "--json",
            json.dumps(access_control, sort_keys=True),
            "--output",
            "json",
        )
    )

    permissions = run_json(
        workspace_command(
            args,
            "workspace",
            "get-permissions",
            "directories",
            object_id,
            "--output",
            "json",
        )
    )
    matching = [
        entry
        for entry in permissions.get("access_control_list", [])
        if entry.get("service_principal_name") == args.app_service_principal_client_id
    ]
    can_read = any(
        permission.get("permission_level") in {"CAN_READ", "CAN_RUN", "CAN_EDIT", "CAN_MANAGE"}
        for entry in matching
        for permission in entry.get("all_permissions", [])
    )
    if not can_read:
        raise RuntimeError("App service principal does not have read access to bundle sources")

    print(
        json.dumps(
            {
                "bundle_source_path": source_path,
                "principal_suffix": args.app_service_principal_client_id[-6:],
                "permission": "CAN_READ",
                "verified": True,
            }
        )
    )


def verify_compute_policy_access(args: argparse.Namespace) -> None:
    """Verify the App SP has only the required CAN_USE binding on the managed policy."""

    policy_id = resource_id(args, "cluster_policies", COMPUTE_POLICY_KEY)
    permissions = run_json(
        workspace_command(
            args,
            "permissions",
            "get",
            "cluster-policies",
            policy_id,
            "--output",
            "json",
        )
    )
    matching = [
        entry
        for entry in permissions.get("access_control_list", [])
        if entry.get("service_principal_name") == args.app_service_principal_client_id
    ]
    can_use = any(
        permission.get("permission_level") == "CAN_USE"
        for entry in matching
        for permission in entry.get("all_permissions", [])
    )
    if not can_use:
        raise RuntimeError("App service principal does not have CAN_USE on compute policy")

    # The application ID is an identifier rather than a secret, but truncation keeps routine
    # deployment output free of complete identity values.
    print(
        json.dumps(
            {
                "compute_policy": policy_id,
                "principal_suffix": args.app_service_principal_client_id[-6:],
                "permission": "CAN_USE",
                "verified": True,
            }
        )
    )


def selective_deploy(args: argparse.Namespace, *resources: str) -> None:
    """Deploy an explicit resource phase and its referenced dependencies."""

    run(
        bundle_command(
            "deploy",
            args,
            "--auto-approve",
            "--select",
            ",".join(resources),
        )
    )


def restart_continuous_job(args: argparse.Namespace, resource_key: str) -> None:
    """Restart a continuous job so its next run uses the deployed source and identity."""

    run(bundle_command("run", args, resource_key, "--restart", "--no-wait"))


def active_job_runs(args: argparse.Namespace, resource_key: str) -> list[dict[str, Any]]:
    """Return active runs for one deployed bundle job."""

    job_id = resource_id(args, "jobs", resource_key)
    command = workspace_command(
        args,
        "jobs",
        "list-runs",
        "--job-id",
        job_id,
        "--active-only",
        "--limit",
        "25",
        "--output",
        "json",
    )
    payload = json.loads(run(command, capture=True).stdout)
    # Databricks CLI 1.17 emits a top-level array. Older clients wrapped it in ``{"runs": [...]}``.
    if isinstance(payload, list):
        runs = payload
    elif isinstance(payload, dict):
        runs = payload.get("runs", [])
    else:
        raise RuntimeError(f"Unexpected list-runs response for bundle job {resource_key}")
    if not isinstance(runs, list):
        raise RuntimeError(f"Unexpected active-run response for bundle job {resource_key}")
    return [run_payload for run_payload in runs if isinstance(run_payload, dict)]


def stop_obsolete_continuous_job(args: argparse.Namespace, resource_key: str) -> None:
    """Cancel every active or queued run after the bundle has paused an obsolete job."""

    job_id = resource_id(args, "jobs", resource_key)
    run(
        workspace_command(
            args,
            "jobs",
            "cancel-all-runs",
            "--job-id",
            job_id,
            "--all-queued-runs",
        )
    )

    # Some workspace versions acknowledge cancel-all-runs without terminating an existing
    # CONTINUOUS_RESTART parent. Cancel each observed parent explicitly before polling so a
    # legacy producer can never overlap the replacement Rate source.
    for run_payload in active_job_runs(args, resource_key):
        run_id = run_payload.get("run_id")
        if run_id:
            run(
                workspace_command(
                    args,
                    "jobs",
                    "cancel-run",
                    str(run_id),
                    "--no-wait",
                )
            )

    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline:
        if not active_job_runs(args, resource_key):
            return
        time.sleep(args.poll_seconds)
    raise TimeoutError(
        f"Timed out after {args.timeout_seconds}s stopping obsolete job {resource_key}"
    )


def wait_for_current_rate_run(
    args: argparse.Namespace,
    *,
    restart_requested_at_ms: int,
    previous_run_ids: set[str],
) -> dict[str, Any]:
    """Require a newly started shared Rate RTM parent run to reach RUNNING.

    A RUNNING job by itself does not prove fresh Lakebase telemetry. The authenticated App health
    and sandbox acceptance checks perform that verification after the App has been deployed.
    """

    deadline = time.monotonic() + args.timeout_seconds
    last_states: list[str] = []
    while time.monotonic() < deadline:
        runs = active_job_runs(args, RATE_JOB_KEY)
        last_states = [
            str((run_payload.get("state") or {}).get("life_cycle_state", "UNKNOWN"))
            for run_payload in runs
        ]
        for run_payload in runs:
            state = str((run_payload.get("state") or {}).get("life_cycle_state", "UNKNOWN"))
            start_time = int(run_payload.get("start_time") or 0)
            run_id = str(run_payload.get("run_id") or "")
            # Continuous-job replacement can overlap the old canceling parent. Requiring a start
            # time near this restart request and a new run ID prevents an older RUNNING parent from
            # satisfying the gate while allowing for small client/workspace clock skew.
            if (
                state == "RUNNING"
                and run_id not in previous_run_ids
                and start_time >= restart_requested_at_ms - 60_000
            ):
                return run_payload
        time.sleep(args.poll_seconds)
    raise TimeoutError(
        f"Timed out after {args.timeout_seconds}s waiting for {RATE_JOB_KEY}; "
        f"last active states were {last_states or ['none']}"
    )


def deploy(args: argparse.Namespace) -> None:
    """Execute the ordered, idempotent data-plane deployment protocol."""

    # Phase 1 runs as the deployer because this setup job intentionally has no run_as block.
    # Continuous workloads retain their previous identity until all prerequisite grants exist.
    selective_deploy(
        args,
        "cluster_policies.asset_reliability_workloads",
        "jobs.setup_runtime_principal_access",
        f"jobs.{LAKEBASE_MIGRATION_KEY}",
    )
    run(bundle_command("run", args, "setup_runtime_principal_access"))
    verify_compute_policy_access(args)
    grant_bundle_source_read(args)
    # This checked run is the hard schema barrier. No workload replacement or cancellation below
    # is reachable if App-SP authentication, migration DDL, or contract verification fails.
    run(bundle_command("run", args, LAKEBASE_MIGRATION_KEY))

    # The full resource deployment changes the legacy continuous resources to PAUSED before they
    # are canceled. The governed-history assets remain available as a production reference, but
    # are not part of the active synthetic demo or its readiness gate.
    run(bundle_command("deploy", args, "--auto-approve"))
    grant_bundle_source_read(args)
    for resource_key in OBSOLETE_CONTINUOUS_JOB_KEYS:
        stop_obsolete_continuous_job(args, resource_key)
    previous_rate_run_ids = {
        str(run_payload.get("run_id"))
        for run_payload in active_job_runs(args, RATE_JOB_KEY)
        if run_payload.get("run_id") is not None
    }
    restart_requested_at_ms = int(time.time() * 1000)
    restart_continuous_job(args, RATE_JOB_KEY)
    active_rate_run = wait_for_current_rate_run(
        args,
        restart_requested_at_ms=restart_requested_at_ms,
        previous_run_ids=previous_rate_run_ids,
    )
    print(
        json.dumps(
            {
                "rate_rtm_job": RATE_JOB_KEY,
                "run_id": active_rate_run.get("run_id"),
                "start_time": active_rate_run.get("start_time"),
                "life_cycle_state": "RUNNING",
                "telemetry_freshness_verified": False,
                "next_gate": "authenticated App health and sandbox telemetry acceptance",
            },
            sort_keys=True,
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Explicit Databricks CLI profile.")
    parser.add_argument("--target", default="dev", help="Bundle target (default: dev).")
    parser.add_argument(
        "--access-group",
        default=DEFAULT_ACCESS_GROUP,
        help="Existing App-admission group; this helper never creates groups or product roles.",
    )
    parser.add_argument(
        "--app-service-principal-client-id",
        default="bd5178cb-8774-4d77-bc63-b4f491716d0b",
        help="Existing Databricks App service-principal application ID.",
    )
    parser.add_argument(
        "--catalog",
        default="classic_stable_realtime_demos_catalog",
        help="Catalog containing the governed telemetry view.",
    )
    parser.add_argument(
        "--schema",
        default="asset_reliability_command_center",
        help="Schema containing the governed telemetry view.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Maximum wait for obsolete jobs to stop and Rate RTM to reach RUNNING.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=10.0,
        help="Continuous-job state polling interval.",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Execute the full ordered deployment; without this flag only validation runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(bundle_command("validate", args))
    if args.deploy:
        deploy(args)


if __name__ == "__main__":
    main()
