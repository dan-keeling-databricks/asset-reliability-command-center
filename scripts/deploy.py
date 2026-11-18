"""Checked manual deployment for the complete demo or only its Databricks App.

The project intentionally has no CI/CD service.  This entry point gives a human deployer one
repeatable path that validates a clean source revision, produces current frontend assets,
injects build provenance, and verifies the live platform state.  The default complete-demo scope
reconciles bundle and governance resources and restarts continuous workloads.  ``--app-only``
instead updates only the staged Databricks App snapshot for changes contained within the App
runtime boundary.

For a complete deployment, the data-plane phase migrates Lakebase and starts the shared Spark
Rate RTM job before the App snapshot is created. ``preflight`` is read-only; only explicit
``install`` and ``update`` operations mutate the workspace. No credential or generated secret is
read, printed, or written by this script; Databricks CLI authentication is selected only through
the required explicit profile.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUILD_INFO = ROOT / ".build-info.json"
MINIMUM_CLI_VERSION = (1, 17, 0)

# The App snapshot is intentionally smaller than the bundle. Keep this allowlist aligned with
# actual runtime imports and file reads; jobs, notebooks and optional reference assets stay out.
APP_RUNTIME_DIRECTORIES = (
    Path("backend"),
    Path("frontend/dist"),
)
APP_RUNTIME_FILES = (
    Path("sql/lakebase_schema.sql"),
    Path("pyproject.toml"),
    Path("uv.lock"),
    Path("requirements.txt"),
    Path(".build-info.json"),
)
APP_CONFIG_TEMPLATE = Path("app.yaml.template")
APP_IGNORED_NAMES = {".DS_Store", ".databrickscfg", ".env", "__pycache__"}
APP_IGNORED_SUFFIXES = {".pyc", ".pyo"}
APP_FORBIDDEN_TOP_LEVEL = {
    ".databricks",
    ".git",
    ".venv",
    "docs",
    "genie",
    "scripts",
    "src",
    "tests",
}


@dataclass(frozen=True)
class DeploymentInputs:
    """Explicit environment identity required by every remote deployment command."""

    profile: str
    target: str
    expected_host: str
    access_group: str
    catalog: str
    schema: str
    volume: str
    warehouse_id: str
    workload_node_type: str
    app_name: str
    app_service_principal_client_id: str | None
    lakebase_project_id: str
    lakebase_branch: str
    lakebase_database: str
    lakebase_database_resource_name: str
    lakebase_endpoint_name: str
    lakebase_schema: str
    foundation_model: str
    model_service_name: str
    mlflow_experiment_name: str
    mlflow_trace_catalog: str
    mlflow_trace_schema: str
    mlflow_trace_table_prefix: str
    sandbox_capacity: int
    telemetry_samples_per_second: int
    telemetry_source_partitions: int
    skill_id: str
    mcp_service_id: str
    mcp_connection_id: str
    policy_function_name: str
    global_agent_concurrency: int
    table_timeout_seconds: int

    @property
    def schema_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def skill_fqn(self) -> str:
        return f"{self.schema_fqn}.{self.skill_id}"

    @property
    def mcp_service_fqn(self) -> str:
        return f"{self.schema_fqn}.{self.mcp_service_id}"

    @property
    def policy_fqn(self) -> str:
        return f"{self.schema_fqn}.{self.policy_function_name}"

    def with_app_principal(self, client_id: str) -> DeploymentInputs:
        """Return immutable inputs bound to the App identity discovered in the workspace."""

        return replace(self, app_service_principal_client_id=client_id)

    @property
    def bundle_flags(self) -> list[str]:
        variables: dict[str, str | int] = {
            "app_access_group": self.access_group,
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "warehouse_id": self.warehouse_id,
            "workload_node_type": self.workload_node_type,
            "lakebase_project_id": self.lakebase_project_id,
            "lakebase_branch_name": self.lakebase_branch,
            "lakebase_database_name": self.lakebase_database,
            "lakebase_database_resource_name": self.lakebase_database_resource_name,
            "lakebase_endpoint_name": self.lakebase_endpoint_name,
            "lakebase_schema": self.lakebase_schema,
            "foundation_model": self.foundation_model,
            "model_service_name": self.model_service_name,
            "mlflow_experiment_name": self.mlflow_experiment_name,
            "mlflow_trace_catalog": self.mlflow_trace_catalog,
            "mlflow_trace_schema": self.mlflow_trace_schema,
            "mlflow_trace_table_prefix": self.mlflow_trace_table_prefix,
            "mcp_service_name": self.mcp_service_id,
            "governed_skill_name": self.skill_id,
            "policy_function_name": self.policy_function_name,
            "global_agent_concurrency": self.global_agent_concurrency,
            "sandbox_capacity": self.sandbox_capacity,
            "telemetry_samples_per_second": self.telemetry_samples_per_second,
            "telemetry_source_partitions": self.telemetry_source_partitions,
            "app_name": self.app_name,
            "workspace_host": self.expected_host.rstrip("/"),
        }
        flags = ["--target", self.target, "--profile", self.profile]
        for name, value in variables.items():
            flags.extend(["--var", f"{name}={value}"])
        return flags


class DeploymentOperation(StrEnum):
    """Customer-facing lifecycle operations with explicit mutation semantics."""

    PREFLIGHT = "preflight"
    INSTALL = "install"
    UPDATE = "update"


@dataclass(frozen=True)
class Step:
    """One ordered remote mutation with an optional environment override."""

    label: str
    command: list[str]
    environment: dict[str, str] | None = None


class Runner:
    """Checked subprocess boundary that never invokes a shell."""

    def run(
        self,
        command: list[str],
        *,
        capture: bool = False,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        print(f"+ {shlex.join(command)}", flush=True)
        env = os.environ.copy()
        if environment:
            env.update(environment)
        return subprocess.run(  # noqa: S603
            command,
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=capture,
            env=env,
        )

    def json(self, command: list[str]) -> dict[str, Any]:
        result = self.run(command, capture=True)
        return json.loads(result.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        nargs="?",
        choices=[item.value for item in DeploymentOperation],
        default=DeploymentOperation.PREFLIGHT.value,
        help="preflight is read-only; install/update apply the declared workspace changes.",
    )
    parser.add_argument("--profile", required=True, help="Databricks CLI profile.")
    parser.add_argument("--target", required=True, help="Bundle target.")
    parser.add_argument(
        "--expected-host",
        required=True,
        help="Exact workspace URL expected from the selected profile.",
    )
    parser.add_argument(
        "--access-group",
        required=True,
        help="Existing workspace group admitted to the App without assigning a product role.",
    )
    parser.add_argument("--catalog", required=True, help="Project Unity Catalog catalog.")
    parser.add_argument("--schema", required=True, help="Project Unity Catalog schema.")
    parser.add_argument("--volume", required=True, help="Managed checkpoint volume name.")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse resource ID.")
    parser.add_argument(
        "--workload-node-type",
        required=True,
        help="Available classic compute node type selected for Spark workloads.",
    )
    parser.add_argument("--app-name", required=True, help="Databricks App name.")
    parser.add_argument(
        "--app-service-principal-client-id",
        help=(
            "Optional expected App service-principal application ID. The deployed App is the "
            "source of truth and its ID is discovered automatically."
        ),
    )
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
    parser.add_argument("--sandbox-capacity", required=True, type=int)
    parser.add_argument("--telemetry-samples-per-second", required=True, type=int)
    parser.add_argument("--telemetry-source-partitions", required=True, type=int)
    parser.add_argument("--skill-id", required=True)
    parser.add_argument("--mcp-service-id", required=True)
    parser.add_argument("--mcp-connection-id", required=True)
    parser.add_argument("--policy-function-name", required=True)
    parser.add_argument("--global-agent-concurrency", required=True, type=int)
    parser.add_argument(
        "--app-only",
        action="store_true",
        help=(
            "Deploy only the staged Databricks App snapshot; skip Lakebase migration, "
            "data-plane workloads, Genie, Skill, MCP, and Unity Gateway reconciliation."
        ),
    )
    parser.add_argument(
        "--table-timeout-seconds",
        type=int,
        default=600,
        help="Maximum wait for the Rate RTM workload to reach RUNNING.",
    )
    return parser.parse_args()


def current_commit(runner: Runner) -> str:
    """Return the exact immutable revision selected for deployment."""

    return runner.run(["git", "rev-parse", "HEAD"], capture=True).stdout.strip()


def assert_clean_head(runner: Runner) -> None:
    """Reject tracked, staged, or untracked source changes before artifact generation."""

    status = runner.run(
        ["git", "status", "--porcelain", "--untracked-files=all"], capture=True
    ).stdout
    unexpected = [
        line
        for line in status.splitlines()
        if not line.endswith(" .build-info.json")
    ]
    if unexpected:
        raise RuntimeError(
            "Deployment requires a clean HEAD; commit or remove these changes:\n"
            + "\n".join(unexpected)
        )


def write_build_info(commit: str, path: Path = BUILD_INFO) -> None:
    """Materialize source provenance consumed by the App's build-info endpoint."""

    payload = {"git_commit": commit}
    path.write_text(json.dumps(payload, sort_keys=True) + "\n")
    if json.loads(path.read_text()) != payload:
        raise RuntimeError(f"Unable to verify generated build metadata at {path}")


def _ignored_app_files(_directory: str, names: list[str]) -> set[str]:
    """Exclude interpreter caches and local filesystem metadata from the App artifact."""

    return {
        name
        for name in names
        if name in APP_IGNORED_NAMES or Path(name).suffix in APP_IGNORED_SUFFIXES
    }


def stage_app_source(
    root: Path = ROOT,
    destination: Path | None = None,
    inputs: DeploymentInputs | None = None,
) -> Path:
    """Create a deterministic, runtime-only source tree for the Databricks App snapshot.

    Bundle resources such as Genie spaces, pipeline notebooks, and job notebooks remain rooted in
    the repository and are deployed independently.  The App is snapshotted only from this staged
    allowlist, preventing unsupported special workspace nodes and development files from entering
    the App import.
    """

    root = root.resolve()
    destination = (destination or root / ".artifacts" / "app").resolve()
    if destination == root or root not in destination.parents:
        raise RuntimeError(f"App staging destination must be below {root}: {destination}")

    required = [*(root / path for path in APP_RUNTIME_DIRECTORIES)]
    required.extend(root / path for path in APP_RUNTIME_FILES)
    required.append(root / APP_CONFIG_TEMPLATE)
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    if missing:
        raise RuntimeError("App runtime inputs are missing: " + ", ".join(missing))

    for source in required:
        if source.is_symlink() or any(path.is_symlink() for path in source.rglob("*")):
            raise RuntimeError(f"App runtime input contains a symlink: {source}")

    if destination.is_symlink():
        raise RuntimeError(f"Refusing to replace symlinked App staging directory: {destination}")
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)

    for relative in APP_RUNTIME_DIRECTORIES:
        shutil.copytree(
            root / relative,
            destination / relative,
            ignore=_ignored_app_files,
        )
    for relative in APP_RUNTIME_FILES:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / relative, target)
    if inputs:
        generate_app_config(root / APP_CONFIG_TEMPLATE, destination / "app.yaml", inputs)

    staged_top_level = {path.name for path in destination.iterdir()}
    forbidden = sorted(staged_top_level & APP_FORBIDDEN_TOP_LEVEL)
    genie_definitions = sorted(destination.rglob("*.geniespace.json"))
    ignored_residue = sorted(
        path
        for path in destination.rglob("*")
        if path.name in APP_IGNORED_NAMES or path.suffix in APP_IGNORED_SUFFIXES
    )
    if forbidden or genie_definitions or ignored_residue:
        raise RuntimeError(
            "Unsafe files entered the App staging directory: "
            f"top_level={forbidden}, genie={genie_definitions}, ignored={ignored_residue}"
        )

    return destination


def generated_app_environment(inputs: DeploymentInputs) -> dict[str, str]:
    """Return portable App values derived only from explicit deployment inputs."""

    host = inputs.expected_host.rstrip("/")
    return {
        "MLFLOW_TRACING_DESTINATION": (
            f"{inputs.mlflow_trace_catalog}.{inputs.mlflow_trace_schema}"
        ),
        "GOVERNED_MCP_ENDPOINT": (
            f"{host}/ai-gateway/mcp-services/{inputs.mcp_service_fqn}"
        ),
        "GOVERNED_SKILL_ENDPOINT": (
            f"{host}/ai-gateway/skills/?schema={inputs.schema_fqn}"
        ),
        "GOVERNED_SKILL_TOOL": f"skill_{inputs.skill_fqn}",
        "GOVERNED_SKILL_DOCUMENT_URL": (
            f"{host}/api/2.0/fs/files/Skills/{inputs.catalog}/{inputs.schema}/"
            f"{inputs.skill_id}/SKILL.md"
        ),
        "LAKEBASE_SCHEMA": inputs.lakebase_schema,
        "GLOBAL_AGENT_CONCURRENCY": str(inputs.global_agent_concurrency),
    }


def generate_app_config(
    template: Path, destination: Path, inputs: DeploymentInputs
) -> None:
    """Render environment-specific ``app.yaml`` only inside the ignored App artifact."""

    if template.resolve() == destination.resolve():
        raise RuntimeError("Generated app.yaml must not overwrite the tracked template")
    payload = yaml.safe_load(template.read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("env"), list):
        raise RuntimeError(f"Invalid Databricks App template: {template}")
    declared = {
        item.get("name"): item
        for item in payload["env"]
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    for name, value in generated_app_environment(inputs).items():
        entry = declared.get(name)
        if entry is None:
            entry = {"name": name}
            payload["env"].append(entry)
        entry.pop("valueFrom", None)
        entry["value"] = value
    destination.write_text(yaml.safe_dump(payload, sort_keys=False))


def assert_cli_version(runner: Runner) -> None:
    """Enforce the repository's minimum supported Databricks CLI version."""

    output = runner.run(["databricks", "version"], capture=True).stdout.strip()
    numeric = output.rsplit(" ", maxsplit=1)[-1].removeprefix("v").split("-")[0]
    try:
        version = tuple(int(part) for part in numeric.split(".")[:3])
    except ValueError as error:
        raise RuntimeError(f"Cannot parse Databricks CLI version: {output}") from error
    if version < MINIMUM_CLI_VERSION:
        required = ".".join(str(part) for part in MINIMUM_CLI_VERSION)
        raise RuntimeError(f"Databricks CLI {required} or later is required; found {numeric}")


def verify_profile(
    runner: Runner, inputs: DeploymentInputs, *, require_app: bool = True
) -> tuple[dict[str, Any], dict[str, Any] | None, DeploymentInputs]:
    """Confirm workspace/group access and optionally resolve an existing App identity."""

    auth = runner.json(
        ["databricks", "auth", "describe", "--profile", inputs.profile, "-o", "json"]
    )
    actual_host = auth.get("details", {}).get("host", "").rstrip("/")
    if actual_host != inputs.expected_host.rstrip("/"):
        raise RuntimeError(
            f"Profile {inputs.profile!r} resolves to {actual_host!r}, not "
            f"{inputs.expected_host!r}"
        )

    user = runner.json(
        ["databricks", "current-user", "me", "--profile", inputs.profile, "-o", "json"]
    )
    groups = {
        value
        for group in user.get("groups", [])
        for value in (group.get("display"), group.get("value"))
        if value
    }
    if inputs.access_group not in groups:
        raise RuntimeError(
            f"Deploying user is not a member of existing group {inputs.access_group!r}"
        )
    try:
        app = fetch_app(runner, inputs)
    except subprocess.CalledProcessError:
        if require_app:
            raise RuntimeError(
                f"App {inputs.app_name!r} does not exist; use the install operation first"
            ) from None
        return user, None, inputs
    actual_principal = app.get("service_principal_client_id")
    if not actual_principal:
        raise RuntimeError(
            f"App {inputs.app_name!r} does not expose a service-principal client ID"
        )
    if (
        inputs.app_service_principal_client_id
        and actual_principal != inputs.app_service_principal_client_id
    ):
        raise RuntimeError(
            f"App {inputs.app_name!r} uses service principal {actual_principal!r}, not the "
            "explicit deployment input"
        )
    return user, app, inputs.with_app_principal(str(actual_principal))


def fetch_app(runner: Runner, inputs: DeploymentInputs) -> dict[str, Any]:
    """Fetch the App through the explicitly selected profile."""

    return runner.json(
        [
            "databricks",
            "apps",
            "get",
            inputs.app_name,
            "--profile",
            inputs.profile,
            "-o",
            "json",
        ]
    )


def bind_app_identity(inputs: DeploymentInputs, app: dict[str, Any]) -> DeploymentInputs:
    """Resolve and optionally verify the App-created service principal."""

    actual = str(app.get("service_principal_client_id") or "")
    if not actual:
        raise RuntimeError(f"App {inputs.app_name!r} has no service-principal client ID")
    if inputs.app_service_principal_client_id and inputs.app_service_principal_client_id != actual:
        raise RuntimeError(
            f"App {inputs.app_name!r} uses service principal {actual!r}, not the explicit input"
        )
    return inputs.with_app_principal(actual)


def resolve_app_source_path(app: dict[str, Any]) -> str:
    """Return the existing narrow bundle path that may receive an App-only sync."""

    source_path = str(app.get("default_source_code_path") or "")
    expected_suffix = "/files/.artifacts/app"
    if not source_path.startswith("/Workspace/Users/") or not source_path.endswith(
        expected_suffix
    ):
        raise RuntimeError(
            "App-only deployment requires the existing App source path below a user bundle "
            f"and ending in {expected_suffix!r}; found {source_path!r}"
        )
    return source_path


def assert_requirements_current(runner: Runner) -> None:
    """Ensure the committed App requirements are a deterministic export of uv.lock."""

    exported = runner.run(
        [
            "uv",
            "export",
            "--locked",
            "--no-dev",
            "--no-hashes",
            "--format",
            "requirements-txt",
        ],
        capture=True,
    ).stdout
    committed = (ROOT / "requirements.txt").read_text()
    # uv embeds the exact invocation in the two-line generated header.  Compare the package
    # body so stdout export and the committed ``-o requirements.txt`` export are equivalent.
    if exported.splitlines()[2:] != committed.splitlines()[2:]:
        raise RuntimeError(
            "requirements.txt is stale; regenerate it with the uv export command in the runbook"
        )


def assert_frontend_fresh(root: Path = ROOT) -> None:
    """Prove the ignored production bundle is newer than every frontend source input."""

    frontend = root / "frontend"
    index = frontend / "dist" / "index.html"
    assets = list((frontend / "dist" / "assets").glob("*"))
    if not index.is_file() or not assets:
        raise RuntimeError("frontend/dist is incomplete; run the production frontend build")
    inputs = [
        path
        for path in frontend.rglob("*")
        if path.is_file()
        and "dist" not in path.parts
        and "node_modules" not in path.parts
        and not path.name.endswith(".tsbuildinfo")
    ]
    newest_input = max(path.stat().st_mtime_ns for path in inputs)
    oldest_output = min(path.stat().st_mtime_ns for path in [index, *assets])
    if oldest_output < newest_input:
        raise RuntimeError("frontend/dist is older than frontend source inputs")


def run_quality_gate(runner: Runner, inputs: DeploymentInputs) -> None:
    """Install locked dependencies, run all tests, build assets, and validate the bundle."""

    commands = [
        ["uv", "sync", "--locked"],
        ["npm", "--prefix", "frontend", "ci"],
        ["uv", "run", "ruff", "check", "backend", "tests", "src", "scripts"],
        ["uv", "run", "pytest"],
        ["npm", "--prefix", "frontend", "run", "typecheck"],
        ["npm", "--prefix", "frontend", "run", "test:unit"],
        ["npm", "--prefix", "frontend", "run", "build"],
    ]
    for command in commands:
        runner.run(command)
    assert_requirements_current(runner)
    assert_frontend_fresh()
    staged = stage_app_source(inputs=inputs)
    print(f"Staged runtime-only App source at {staged.relative_to(ROOT)}", flush=True)
    runner.run(["databricks", "bundle", "validate", "--strict", *inputs.bundle_flags])
    runner.run(["git", "diff", "--exit-code"])


def deployment_steps(
    inputs: DeploymentInputs,
    *,
    app_only: bool = False,
    app_source_path: str | None = None,
) -> list[Step]:
    """Return remote mutations for the selected deployment scope."""

    flags = inputs.bundle_flags
    if not app_only and not inputs.app_service_principal_client_id:
        raise RuntimeError(
            "Complete deployment requires the App-created service principal to be resolved"
        )
    app_step = Step(
        "Deploy the App snapshot after Rate telemetry is ready",
        ["databricks", "apps", "deploy", *flags, "--auto-approve"],
    )
    if app_only:
        if not app_source_path:
            raise ValueError("App-only deployment requires the resolved App source path")
        steps = [
            Step(
                "Sync only the staged App runtime source",
                [
                    "databricks",
                    "sync",
                    str(ROOT / ".artifacts" / "app"),
                    app_source_path,
                    "--full",
                    "--include",
                    "**",
                    "--exclude",
                    ".databricks/**",
                    "--profile",
                    inputs.profile,
                ],
            ),
            Step(
                "Create the App snapshot through the direct API",
                [
                    "databricks",
                    "apps",
                    "deploy",
                    inputs.app_name,
                    "--source-code-path",
                    app_source_path,
                    "--mode",
                    "SNAPSHOT",
                    "--profile",
                    inputs.profile,
                ],
            ),
        ]
        assert_app_only_steps(steps, inputs.app_name, app_source_path)
        return steps

    return [
        Step(
            "Deploy the personal-sandbox Rate data plane in readiness-gated phases",
            [
                "uv",
                "run",
                "python",
                "scripts/deploy_data_plane.py",
                "--profile",
                inputs.profile,
                "--target",
                inputs.target,
                "--access-group",
                inputs.access_group,
                "--app-service-principal-client-id",
                inputs.app_service_principal_client_id,
                "--catalog",
                inputs.catalog,
                "--schema",
                inputs.schema,
                "--volume",
                inputs.volume,
                "--warehouse-id",
                inputs.warehouse_id,
                "--workload-node-type",
                inputs.workload_node_type,
                "--workspace-host",
                inputs.expected_host.rstrip("/"),
                "--app-name",
                inputs.app_name,
                "--lakebase-project-id",
                inputs.lakebase_project_id,
                "--lakebase-branch",
                inputs.lakebase_branch,
                "--lakebase-database",
                inputs.lakebase_database,
                "--lakebase-database-resource-name",
                inputs.lakebase_database_resource_name,
                "--lakebase-endpoint-name",
                inputs.lakebase_endpoint_name,
                "--lakebase-schema",
                inputs.lakebase_schema,
                "--foundation-model",
                inputs.foundation_model,
                "--model-service-name",
                inputs.model_service_name,
                "--mlflow-experiment-name",
                inputs.mlflow_experiment_name,
                "--mlflow-trace-catalog",
                inputs.mlflow_trace_catalog,
                "--mlflow-trace-schema",
                inputs.mlflow_trace_schema,
                "--mlflow-trace-table-prefix",
                inputs.mlflow_trace_table_prefix,
                "--mcp-service-name",
                inputs.mcp_service_id,
                "--governed-skill-name",
                inputs.skill_id,
                "--policy-function-name",
                inputs.policy_function_name,
                "--global-agent-concurrency",
                str(inputs.global_agent_concurrency),
                "--sandbox-capacity",
                str(inputs.sandbox_capacity),
                "--telemetry-samples-per-second",
                str(inputs.telemetry_samples_per_second),
                "--telemetry-source-partitions",
                str(inputs.telemetry_source_partitions),
                "--timeout-seconds",
                str(inputs.table_timeout_seconds),
                "--deploy",
            ],
        ),
        Step(
            "Create or update the Unity Gateway policy function",
            ["databricks", "bundle", "run", *flags, "setup_unity_governance"],
        ),
        Step(
            "Reconcile the Skill, MCP source, service, selectors, and ASK policy",
            ["uv", "run", "python", "-m", "scripts.deploy_governance"],
            environment={
                "DATABRICKS_CONFIG_PROFILE": inputs.profile,
                "ASSET_RELIABILITY_CATALOG": inputs.catalog,
                "ASSET_RELIABILITY_SCHEMA": inputs.schema,
                "ASSET_RELIABILITY_APP_NAME": inputs.app_name,
                "ASSET_RELIABILITY_SKILL_ID": inputs.skill_id,
                "ASSET_RELIABILITY_MCP_SERVICE_ID": inputs.mcp_service_id,
                "ASSET_RELIABILITY_MCP_CONNECTION_ID": inputs.mcp_connection_id,
                "ASSET_RELIABILITY_POLICY_FUNCTION_NAME": inputs.policy_function_name,
                "ASSET_RELIABILITY_UC_AUDIENCE": "account users",
            },
        ),
        app_step,
    ]


def install_bootstrap_step(inputs: DeploymentInputs) -> Step:
    """Create bundle foundations, including the App and its generated workload identity."""

    return Step(
        "Create foundational resources and the Databricks App",
        [
            "databricks",
            "bundle",
            "deploy",
            *inputs.bundle_flags,
            "--auto-approve",
            "--select",
            "schemas.project_schema,apps.command_center",
        ],
    )


def assert_app_only_steps(
    steps: list[Step], app_name: str, app_source_path: str
) -> None:
    """Fail closed if an App-only plan contains any non-App mutation."""

    expected = (
        ("databricks", "sync"),
        ("databricks", "apps", "deploy", app_name),
    )
    prefixes = [tuple(step.command[: len(prefix)]) for step, prefix in zip(steps, expected)]
    if len(steps) != 2 or prefixes != list(expected):
        raise RuntimeError("App-only deployment plan contains a non-App mutation")
    for step in steps:
        joined = " ".join(step.command)
        if "bundle" in step.command or "scripts.deploy_" in joined:
            raise RuntimeError("App-only deployment must not invoke bundle or resource tooling")
    if steps[0].command[3] != app_source_path:
        raise RuntimeError("App-only sync target differs from the App's existing source path")


def ensure_app_user_scopes(
    runner: Runner, inputs: DeploymentInputs, *, timeout_seconds: int = 600
) -> None:
    """Restart App compute only when newly declared OBO scopes are not yet effective."""

    get_command = [
        "databricks",
        "apps",
        "get",
        inputs.app_name,
        "--profile",
        inputs.profile,
        "-o",
        "json",
    ]
    required = {"ai-gateway"}
    app = runner.json(get_command)
    effective = set(app.get("effective_user_api_scopes", []))
    if required <= effective:
        return

    runner.run(
        ["databricks", "apps", "stop", inputs.app_name, "--profile", inputs.profile]
    )
    runner.run(
        ["databricks", "apps", "start", inputs.app_name, "--profile", inputs.profile]
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        app = runner.json(get_command)
        effective = set(app.get("effective_user_api_scopes", []))
        state = (app.get("app_status") or {}).get("state")
        if state == "RUNNING" and required <= effective:
            return
        time.sleep(5)
    raise RuntimeError(
        "App did not become RUNNING with the ai-gateway OBO scope"
    )


def verify_live_resources(
    runner: Runner,
    inputs: DeploymentInputs,
    commit: str,
    *,
    app_only: bool = False,
) -> None:
    """Run read-only platform checks appropriate to the deployed scope."""

    runner.run(
        ["databricks", "apps", "get", inputs.app_name, "--profile", inputs.profile]
    )
    if not app_only:
        runner.run(["databricks", "bundle", "summary", *inputs.bundle_flags])
    scope = "App-only deployment" if app_only else "Complete deployment"
    print(
        f"{scope} completed for commit {commit}. Finish the authenticated App and "
        "end-to-end acceptance checklist in docs/DEPLOYMENT.md before recording evidence."
    )


def main() -> None:
    args = parse_args()
    inputs = DeploymentInputs(
        profile=args.profile,
        target=args.target,
        expected_host=args.expected_host,
        access_group=args.access_group,
        catalog=args.catalog,
        schema=args.schema,
        volume=args.volume,
        warehouse_id=args.warehouse_id,
        workload_node_type=args.workload_node_type,
        app_name=args.app_name,
        app_service_principal_client_id=args.app_service_principal_client_id,
        lakebase_project_id=args.lakebase_project_id,
        lakebase_branch=args.lakebase_branch,
        lakebase_database=args.lakebase_database,
        lakebase_database_resource_name=args.lakebase_database_resource_name,
        lakebase_endpoint_name=args.lakebase_endpoint_name,
        lakebase_schema=args.lakebase_schema,
        foundation_model=args.foundation_model,
        model_service_name=args.model_service_name,
        mlflow_experiment_name=args.mlflow_experiment_name,
        mlflow_trace_catalog=args.mlflow_trace_catalog,
        mlflow_trace_schema=args.mlflow_trace_schema,
        mlflow_trace_table_prefix=args.mlflow_trace_table_prefix,
        sandbox_capacity=args.sandbox_capacity,
        telemetry_samples_per_second=args.telemetry_samples_per_second,
        telemetry_source_partitions=args.telemetry_source_partitions,
        skill_id=args.skill_id,
        mcp_service_id=args.mcp_service_id,
        mcp_connection_id=args.mcp_connection_id,
        policy_function_name=args.policy_function_name,
        global_agent_concurrency=args.global_agent_concurrency,
        table_timeout_seconds=args.table_timeout_seconds,
    )
    runner = Runner()

    assert_clean_head(runner)
    commit = current_commit(runner)
    write_build_info(commit)
    assert_cli_version(runner)
    operation = DeploymentOperation(args.operation)
    if args.app_only and operation is DeploymentOperation.INSTALL:
        raise RuntimeError("--app-only is an update path and cannot be used for installation")
    user, app, inputs = verify_profile(
        runner,
        inputs,
        require_app=operation is DeploymentOperation.UPDATE or args.app_only,
    )
    app_source_path = resolve_app_source_path(app) if args.app_only and app else None
    print(
        f"Preflight target: {inputs.expected_host} as "
        f"{user.get('userName', user.get('id', 'unknown user'))}; commit {commit}; "
        f"scope {'App only' if args.app_only else 'complete demo'}"
    )
    run_quality_gate(runner, inputs)

    if operation is DeploymentOperation.PREFLIGHT:
        print(
            "Preflight passed. Re-run with the install or update operation to apply the "
            "selected deployment scope."
        )
        return

    if operation is DeploymentOperation.INSTALL:
        bootstrap = install_bootstrap_step(inputs)
        print(f"\n==> {bootstrap.label}")
        runner.run(bootstrap.command)
        inputs = bind_app_identity(inputs, fetch_app(runner, inputs))

    for step in deployment_steps(
        inputs,
        app_only=args.app_only,
        app_source_path=app_source_path,
    ):
        print(f"\n==> {step.label}")
        runner.run(step.command, environment=step.environment)
        if step.command[:3] == ["databricks", "apps", "deploy"]:
            ensure_app_user_scopes(runner, inputs)
    verify_live_resources(runner, inputs, commit, app_only=args.app_only)


if __name__ == "__main__":
    main()
