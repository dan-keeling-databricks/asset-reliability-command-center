"""Validate and dependency-order deployment of the personal-sandbox data plane.

The helper first grants the App service principal the minimum workload privileges and runs the
idempotent Lakebase migration as that schema-owning identity. It then deploys the primary bundle
and starts the shared Spark Rate Real-Time Mode job. Kafka, Lakeflow and Genie remain outside the
primary bundle as optional production-reference assets.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TIMEOUT_SECONDS = 1_200
COMPUTE_POLICY_KEY = "asset_reliability_workloads"
LAKEBASE_MIGRATION_KEY = "migrate_lakebase_schema"
RATE_JOB_KEY = "telemetry_rtm"


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

    variables = {
        "app_access_group": args.access_group,
        "catalog": args.catalog,
        "schema": args.schema,
        "volume": args.volume,
        "warehouse_id": args.warehouse_id,
        "workload_node_type": args.workload_node_type,
        "workspace_host": args.workspace_host,
        "app_name": args.app_name,
        "lakebase_project_id": args.lakebase_project_id,
        "lakebase_branch_name": args.lakebase_branch,
        "lakebase_database_name": args.lakebase_database,
        "lakebase_database_resource_name": args.lakebase_database_resource_name,
        "lakebase_endpoint_name": args.lakebase_endpoint_name,
        "lakebase_schema": args.lakebase_schema,
        "foundation_model": args.foundation_model,
        "model_service_name": args.model_service_name,
        "mlflow_experiment_name": args.mlflow_experiment_name,
        "mlflow_trace_catalog": args.mlflow_trace_catalog,
        "mlflow_trace_schema": args.mlflow_trace_schema,
        "mlflow_trace_table_prefix": args.mlflow_trace_table_prefix,
        "mcp_service_name": args.mcp_service_name,
        "governed_skill_name": args.governed_skill_name,
        "policy_function_name": args.policy_function_name,
        "global_agent_concurrency": args.global_agent_concurrency,
        "sandbox_capacity": args.sandbox_capacity,
        "telemetry_samples_per_second": args.telemetry_samples_per_second,
        "telemetry_source_partitions": args.telemetry_source_partitions,
    }
    command = [
        "databricks",
        "bundle",
        action,
        "--target",
        args.target,
        "--profile",
        args.profile,
    ]
    for name, value in variables.items():
        command.extend(["--var", f"{name}={value}"])
    return [*command, *extra]


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


def set_continuous_pause_status(
    args: argparse.Namespace, resource_key: str, pause_status: str
) -> None:
    """Pause or resume one continuous job with an explicit profile."""

    if pause_status not in {"PAUSED", "UNPAUSED"}:
        raise ValueError(f"Unsupported continuous pause status: {pause_status}")
    job_id = int(resource_id(args, "jobs", resource_key))
    run(
        workspace_command(
            args,
            "jobs",
            "update",
            "--json",
            json.dumps(
                {
                    "job_id": job_id,
                    "new_settings": {"continuous": {"pause_status": pause_status}},
                },
                sort_keys=True,
            ),
        )
    )


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


def stop_continuous_job(args: argparse.Namespace, resource_key: str) -> None:
    """Explicitly cancel every parent run and wait until the job has no active runs."""

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
    # CONTINUOUS_RESTART parent. Cancel each observed parent explicitly before polling.
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
        f"Timed out after {args.timeout_seconds}s stopping job {resource_key}"
    )


def get_job_run(
    args: argparse.Namespace, run_id: str
) -> dict[str, Any]:
    """Fetch a parent run with the explicitly selected Databricks profile."""

    return run_json(
        workspace_command(
            args,
            "jobs",
            "get-run",
            run_id,
            "--output",
            "json",
        )
    )


def wait_for_current_rate_task(
    args: argparse.Namespace,
    *,
    restart_requested_at_ms: int,
    task_key: str = "ingest_pump_telemetry",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Require the Rate notebook task itself to reach RUNNING in a new parent run.

    A RUNNING job by itself does not prove fresh Lakebase telemetry. The authenticated App health
    and sandbox acceptance checks perform that verification after the App has been deployed.
    """

    deadline = time.monotonic() + args.timeout_seconds
    last_states: list[str] = []
    while time.monotonic() < deadline:
        parents = active_job_runs(args, RATE_JOB_KEY)
        last_states = []
        for parent_summary in parents:
            start_time = int(parent_summary.get("start_time") or 0)
            run_id = str(parent_summary.get("run_id") or "")
            if not run_id or start_time < restart_requested_at_ms - 60_000:
                continue

            parent = get_job_run(args, run_id)
            matching_tasks = [
                task
                for task in parent.get("tasks", [])
                if isinstance(task, dict) and task.get("task_key") == task_key
            ]
            if len(matching_tasks) != 1:
                last_states.append(f"parent={run_id} task={task_key} missing")
                continue
            task = matching_tasks[0]
            task_state = task.get("state") or {}
            life_cycle_state = str(task_state.get("life_cycle_state", "UNKNOWN"))
            state_message = str(task_state.get("state_message", ""))
            last_states.append(
                f"parent={run_id} task={task_key} state={life_cycle_state} "
                f"message={state_message}"
            )
            if life_cycle_state == "RUNNING":
                return parent, task
        time.sleep(args.poll_seconds)
    raise TimeoutError(
        f"Timed out after {args.timeout_seconds}s waiting for {RATE_JOB_KEY}.{task_key}; "
        f"last active states were {last_states or ['none']}"
    )


def deploy(args: argparse.Namespace) -> None:
    """Execute the ordered, idempotent data-plane deployment protocol."""

    # Phase 1 runs as the deployer because this setup job intentionally has no run_as block.
    # Continuous workloads retain their previous identity until all prerequisite grants exist.
    selective_deploy(
        args,
        "volumes.runtime_volume",
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

    # Reconcile only the primary personal-sandbox bundle. Optional production-reference resources
    # are defined in an un-included fragment and cannot enter this readiness path.
    run(bundle_command("deploy", args, "--auto-approve"))
    # Experiment creation provisions the UC trace tables during the full bundle deployment. Grant
    # access only after those tables exist; the App identity writes traces and admitted users read
    # them through MLflow. This setup job is idempotent and blocks App deployment on failure.
    run(bundle_command("run", args, "setup_agent_observability_access"))
    grant_bundle_source_read(args)
    # Pause the continuous trigger before cancellation so Databricks cannot replace an old parent
    # while it is being drained. Only after active_job_runs is empty do we resume the trigger and
    # request a fresh parent/task pair.
    set_continuous_pause_status(args, RATE_JOB_KEY, "PAUSED")
    stop_continuous_job(args, RATE_JOB_KEY)
    restart_requested_at_ms = int(time.time() * 1000)
    set_continuous_pause_status(args, RATE_JOB_KEY, "UNPAUSED")
    active_rate_run, active_rate_task = wait_for_current_rate_task(
        args,
        restart_requested_at_ms=restart_requested_at_ms,
    )
    print(
        json.dumps(
            {
                "rate_rtm_job": RATE_JOB_KEY,
                "run_id": active_rate_run.get("run_id"),
                "start_time": active_rate_run.get("start_time"),
                "task_key": active_rate_task.get("task_key"),
                "task_run_id": active_rate_task.get("run_id"),
                "task_life_cycle_state": "RUNNING",
                "telemetry_freshness_verified": False,
                "next_gate": "authenticated App health and sandbox telemetry acceptance",
            },
            sort_keys=True,
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Explicit Databricks CLI profile.")
    parser.add_argument("--target", required=True, help="Bundle target.")
    parser.add_argument(
        "--access-group",
        required=True,
        help="Existing App-admission group; this helper never creates groups or product roles.",
    )
    parser.add_argument(
        "--app-service-principal-client-id",
        required=True,
        help="Existing Databricks App service-principal application ID.",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Catalog containing the governed telemetry view.",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Schema containing the governed telemetry view.",
    )
    parser.add_argument("--volume", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--workload-node-type", required=True)
    parser.add_argument("--workspace-host", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--lakebase-project-id", required=True)
    parser.add_argument("--lakebase-branch", required=True)
    parser.add_argument("--lakebase-database", required=True)
    parser.add_argument("--lakebase-database-resource-name", required=True)
    parser.add_argument("--lakebase-endpoint-name", required=True)
    parser.add_argument("--lakebase-schema", required=True)
    parser.add_argument("--foundation-model", required=True)
    parser.add_argument("--model-service-name", required=True)
    parser.add_argument("--mlflow-experiment-name", required=True)
    parser.add_argument("--mlflow-trace-catalog", required=True)
    parser.add_argument("--mlflow-trace-schema", required=True)
    parser.add_argument("--mlflow-trace-table-prefix", required=True)
    parser.add_argument("--mcp-service-name", required=True)
    parser.add_argument("--governed-skill-name", required=True)
    parser.add_argument("--policy-function-name", required=True)
    parser.add_argument("--global-agent-concurrency", required=True, type=int)
    parser.add_argument("--sandbox-capacity", required=True, type=int)
    parser.add_argument("--telemetry-samples-per-second", required=True, type=int)
    parser.add_argument("--telemetry-source-partitions", required=True, type=int)
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Maximum wait for the Rate RTM workload to reach RUNNING.",
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
