"""Static and unit contracts for the checked manual deployment entry point."""

from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from databricks.sdk.service.iam import GrantRule

from scripts.deploy import (
    DeploymentInputs,
    _service_principal_user_rules,
    assert_app_only_steps,
    assert_frontend_fresh,
    bind_app_identity,
    deployment_steps,
    generated_app_environment,
    install_app_dependencies_step,
    install_bootstrap_step,
    install_schema_step,
    resolve_app_source_path,
    stage_app_source,
    write_build_info,
)


def inputs() -> DeploymentInputs:
    return DeploymentInputs(
        profile="explicit-profile",
        target="dev",
        expected_host="https://workspace.example",
        access_group="existing-group",
        catalog="demo_catalog",
        schema="asset_reliability",
        volume="runtime",
        warehouse_id="warehouse-123",
        workload_node_type="Standard_D8s_v5",
        app_name="asset-reliability-center",
        app_service_principal_client_id="existing-app-sp",
        lakebase_project_id="customer-project",
        lakebase_branch="production",
        lakebase_database="asset-reliability-db",
        lakebase_database_resource_name="asset-reliability-db-resource",
        lakebase_endpoint_name="primary",
        lakebase_schema="asset_reliability",
        foundation_model="system.ai.databricks-claude-sonnet-4-5",
        model_service_name="asset_reliability_agent_model",
        mlflow_experiment_name="/Shared/customer-agent",
        mlflow_trace_catalog="demo_catalog",
        mlflow_trace_schema="agent_traces",
        mlflow_trace_table_prefix="customer_agent",
        mlflow_trace_reader_principal="customer-trace-readers",
        sandbox_capacity=40,
        telemetry_samples_per_second=2,
        telemetry_source_partitions=4,
        skill_id="asset-reliability-response",
        mcp_service_id="asset_reliability_mcp",
        mcp_connection_id="asset_reliability_mcp_connection",
        policy_function_name="submit_work_order_policy",
        global_agent_concurrency=20,
        table_timeout_seconds=900,
    )


def test_deployment_steps_are_explicit_and_dependency_ordered() -> None:
    steps = deployment_steps(inputs())
    labels = [step.label for step in steps]

    assert labels == [
        "Deploy the personal-sandbox Rate data plane in readiness-gated phases",
        "Create or update the Unity Gateway policy function",
        "Deploy the App snapshot after Rate telemetry is ready",
        "Reconcile the Skill, MCP source, service, selectors, and ASK policy",
    ]
    for step in steps:
        if step.command[0] == "databricks":
            assert "--profile" in step.command
            assert "explicit-profile" in step.command
    assert steps[3].environment == {
        "DATABRICKS_CONFIG_PROFILE": "explicit-profile",
        "ASSET_RELIABILITY_CATALOG": "demo_catalog",
        "ASSET_RELIABILITY_SCHEMA": "asset_reliability",
        "ASSET_RELIABILITY_APP_NAME": "asset-reliability-center",
        "ASSET_RELIABILITY_SKILL_ID": "asset-reliability-response",
        "ASSET_RELIABILITY_MCP_SERVICE_ID": "asset_reliability_mcp",
        "ASSET_RELIABILITY_MCP_CONNECTION_ID": "asset_reliability_mcp_connection",
        "ASSET_RELIABILITY_POLICY_FUNCTION_NAME": "submit_work_order_policy",
        "ASSET_RELIABILITY_UC_AUDIENCE": "account users",
    }
    assert steps[3].command == [
        "uv",
        "run",
        "python",
        "-m",
        "scripts.deploy_governance",
    ]
    assert "--app-service-principal-client-id" in steps[0].command
    assert "existing-app-sp" in steps[0].command
    assert "--timeout-seconds" in steps[0].command
    assert "900" in steps[0].command
    assert "--workload-node-type" in steps[0].command
    assert "Standard_D8s_v5" in steps[0].command


def test_fresh_install_bootstraps_app_before_resolving_its_identity() -> None:
    unresolved = replace(inputs(), app_service_principal_client_id=None)

    schema_step = install_schema_step(unresolved)
    dependencies_step = install_app_dependencies_step(unresolved)
    step = install_bootstrap_step(unresolved)

    assert schema_step.command[-2:] == ["--select", "schemas.project_schema"]
    assert dependencies_step.command[-2:] == [
        "--select",
        (
            "postgres_projects.asset_reliability_postgres,"
            "model_services.agent_model,experiments.agent_observability"
        ),
    ]
    assert step.command[:3] == ["databricks", "bundle", "deploy"]
    assert step.command[-2:] == ["--select", "apps.command_center"]
    assert not any("app_service_principal_client_id" in item for item in step.command)
    assert "lakebase_branch_name=production" in step.command
    assert "lakebase_database_name=asset-reliability-db" in step.command
    assert "workload_node_type=Standard_D8s_v5" in step.command


def test_app_identity_is_discovered_and_optional_expectation_is_verified() -> None:
    unresolved = replace(inputs(), app_service_principal_client_id=None)
    assert (
        bind_app_identity(unresolved, {"service_principal_client_id": "workspace-created-sp"})
        .app_service_principal_client_id
        == "workspace-created-sp"
    )
    with pytest.raises(RuntimeError, match="not the explicit input"):
        bind_app_identity(
            inputs(), {"service_principal_client_id": "different-workspace-sp"}
        )


def test_service_principal_user_role_preserves_existing_grants() -> None:
    rules, changed = _service_principal_user_rules(
        [
            GrantRule(
                role="roles/servicePrincipal.manager",
                principals=["users/operator@example.com"],
            )
        ],
        "users/operator@example.com",
    )

    assert changed is True
    assert [rule.as_dict() for rule in rules] == [
        {
            "role": "roles/servicePrincipal.manager",
            "principals": ["users/operator@example.com"],
        },
        {
            "role": "roles/servicePrincipal.user",
            "principals": ["users/operator@example.com"],
        },
    ]
    unchanged, changed_again = _service_principal_user_rules(
        rules, "users/operator@example.com"
    )
    assert changed_again is False
    assert [rule.as_dict() for rule in unchanged] == [rule.as_dict() for rule in rules]


def test_app_only_deployment_mutates_only_the_app() -> None:
    source_path = "/Workspace/Users/operator/.bundle/project/dev/files/.artifacts/app"
    steps = deployment_steps(
        inputs(), app_only=True, app_source_path=source_path
    )

    assert [step.label for step in steps] == [
        "Sync only the staged App runtime source",
        "Create the App snapshot through the direct API",
    ]
    assert steps[0].command == [
        "databricks",
        "sync",
        str(Path(__file__).resolve().parents[1] / ".artifacts" / "app"),
        source_path,
        "--full",
        "--include",
        "**",
        "--exclude",
        ".databricks/**",
        "--profile",
        "explicit-profile",
    ]
    assert steps[1].command == [
        "databricks",
        "apps",
        "deploy",
        "asset-reliability-center",
        "--source-code-path",
        source_path,
        "--mode",
        "SNAPSHOT",
        "--profile",
        "explicit-profile",
    ]


def test_app_only_source_path_must_be_the_narrow_existing_bundle_path() -> None:
    app = {
        "default_source_code_path": (
            "/Workspace/Users/operator/.bundle/project/dev/files/.artifacts/app"
        )
    }
    assert resolve_app_source_path(app) == app["default_source_code_path"]

    for unsafe_path in ("", "/Workspace/Users/operator", "/Shared/.artifacts/app"):
        with pytest.raises(RuntimeError, match="App-only deployment requires"):
            resolve_app_source_path({"default_source_code_path": unsafe_path})


def test_app_only_plan_rejects_non_app_mutations() -> None:
    source = "/Workspace/Users/operator/.bundle/project/dev/files/.artifacts/app"
    with pytest.raises(RuntimeError, match="non-App mutation"):
        assert_app_only_steps(
            [
                type("StepLike", (), {"command": ["databricks", "bundle", "deploy"]})(),
                type("StepLike", (), {"command": ["databricks", "apps", "deploy"]})(),
            ],
            "asset-reliability-center",
            source,
        )


def test_build_info_contains_only_the_selected_commit(tmp_path: Path) -> None:
    output = tmp_path / ".build-info.json"
    write_build_info("abc123", output)

    assert json.loads(output.read_text()) == {"git_commit": "abc123"}
    assert output.read_text().endswith("\n")


def test_frontend_freshness_rejects_source_newer_than_build(tmp_path: Path) -> None:
    frontend = tmp_path / "frontend"
    source = frontend / "src" / "App.tsx"
    index = frontend / "dist" / "index.html"
    asset = frontend / "dist" / "assets" / "index.js"
    for path in (source, index, asset):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.name)

    assert_frontend_fresh(tmp_path)
    time.sleep(0.001)
    source.write_text("new source")
    with pytest.raises(RuntimeError, match="older than frontend source"):
        assert_frontend_fresh(tmp_path)


def test_app_deployment_gate_runs_frontend_behavioural_tests() -> None:
    """Keep concurrency regression tests in the checked manual deployment path."""

    deploy_source = (Path(__file__).resolve().parents[1] / "scripts/deploy.py").read_text()

    assert '["npm", "--prefix", "frontend", "run", "test:unit"]' in deploy_source


def _write_minimal_runtime(root: Path) -> None:
    """Create only the files that a deployed FastAPI/React App reads at runtime."""

    files = {
        "backend/main.py": "app = object()\n",
        "backend/__pycache__/main.pyc": "cache",
        "backend/.env": "SECRET=must-not-be-staged\n",
        "frontend/dist/index.html": "<main>command center</main>\n",
        "frontend/dist/assets/index.js": "export {};\n",
        "sql/lakebase_schema.sql": "SELECT 1;\n",
        "app.yaml.template": "command: [uvicorn, backend.main:app]\nenv: []\n",
        "pyproject.toml": "[project]\nname='test'\nversion='0.0.0'\n",
        "uv.lock": "version = 1\n",
        "requirements.txt": "fastapi==1.0.0\n",
        ".build-info.json": '{"git_commit":"abc123"}\n',
        "genie/fleet_telemetry_analyst.geniespace.json": "{}\n",
        "src/streaming/job.py": "raise SystemExit\n",
        "tests/test_example.py": "def test_example(): pass\n",
    }
    for relative, contents in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)


def test_app_staging_contains_runtime_allowlist_and_excludes_bundle_resources(
    tmp_path: Path,
) -> None:
    _write_minimal_runtime(tmp_path)

    staged = stage_app_source(tmp_path)

    assert (staged / "backend/main.py").is_file()
    assert (staged / "frontend/dist/index.html").is_file()
    assert (staged / "sql/lakebase_schema.sql").is_file()
    assert json.loads((staged / ".build-info.json").read_text()) == {
        "git_commit": "abc123"
    }
    assert not (staged / "backend/__pycache__").exists()
    assert not (staged / "backend/.env").exists()
    assert not (staged / "genie").exists()
    assert not (staged / "src").exists()
    assert not (staged / "tests").exists()
    assert not list(staged.rglob("*.geniespace.json"))


def test_app_staging_removes_stale_files_before_rebuild(tmp_path: Path) -> None:
    _write_minimal_runtime(tmp_path)
    staged = stage_app_source(tmp_path)
    stale = staged / "stale-secret.env"
    stale.write_text("must not survive\n")

    stage_app_source(tmp_path)

    assert not stale.exists()


def test_app_staging_generates_environment_config_without_changing_template(
    tmp_path: Path,
) -> None:
    _write_minimal_runtime(tmp_path)
    template = tmp_path / "app.yaml.template"
    template.write_text(
        "command: [uvicorn, backend.main:app]\n"
        "env:\n"
        "  - name: GOVERNED_MCP_ENDPOINT\n"
        "    value: maintainer-value\n"
        "  - name: MLFLOW_EXPERIMENT_ID\n"
        "    valueFrom: agent-mlflow-experiment\n"
    )
    original = template.read_text()

    staged = stage_app_source(tmp_path, inputs=inputs())
    rendered = (staged / "app.yaml").read_text()
    rendered_payload = yaml.safe_load(rendered)
    rendered_environment = {
        item["name"]: item for item in rendered_payload["env"]
    }

    assert template.read_text() == original
    for name, value in generated_app_environment(inputs()).items():
        assert rendered_environment[name]["value"] == value
    assert "valueFrom: agent-mlflow-experiment" in rendered
    assert "maintainer-value" not in rendered


def test_bundle_uses_dedicated_app_snapshot_and_opt_in_genie_definition() -> None:
    root = Path(__file__).resolve().parents[1]
    bundle = (root / "databricks.yml").read_text()
    production_reference = (root / "resources" / "production_reference.yml").read_text()

    assert "source_code_path: ./.artifacts/app" in bundle
    assert "production_reference.yml" not in bundle
    assert "file_path: ../genie/fleet_telemetry_analyst.geniespace.json" in production_reference
    assert (root / "genie" / "fleet_telemetry_analyst.geniespace.json.template").is_file()
    assert "- .artifacts/app/**" in bundle
