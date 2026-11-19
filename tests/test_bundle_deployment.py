"""Contracts for the customer-facing, DAB-first deployment."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = ROOT / "databricks.yml"


def bundle() -> dict:
    return yaml.safe_load(BUNDLE_PATH.read_text(encoding="utf-8"))


def test_customer_supplies_only_high_level_workspace_values() -> None:
    variables = bundle()["variables"]
    required = {name for name, config in variables.items() if "default" not in config}

    assert required == {"catalog", "schema", "warehouse_id", "workload_node_type"}
    assert variables["app_access_group"]["default"] == "users"
    assert variables["lakebase_project_id"]["default"] == (
        "asset-reliability-command-center"
    )


def test_bundle_deploy_builds_the_app_and_starts_it() -> None:
    config = bundle()
    app = config["resources"]["apps"]["command_center"]

    assert (ROOT / ".artifacts/app/.gitkeep").is_file()
    assert config["bundle"]["databricks_cli_version"] == ">=1.26.0"
    assert config["artifacts"]["app"]["build"] == "./scripts/build_app.sh"
    assert app["source_code_path"] == "./.artifacts/app"
    assert app["lifecycle"]["started"] is True
    assert app["config"]["command"] == [
        "uvicorn",
        "backend.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
    ]


def test_bundle_deploy_runs_the_required_bootstrap_jobs() -> None:
    resources = bundle()["resources"]
    job_runs = resources["job_runs"]

    assert set(job_runs) == {
        "run_setup_unity_governance",
        "run_setup_agent_observability_access",
        "run_reconcile_governance",
    }
    for run in job_runs.values():
        assert run["lifecycle"]["triggers"] == [{"on_bundle_deploy": True}]

    governance_dependencies = {
        item["resource"]
        for item in job_runs["run_reconcile_governance"]["lifecycle"]["depends_on"]
    }
    assert governance_dependencies == {
        "apps.command_center",
        "job_runs.run_setup_unity_governance",
        "job_runs.run_setup_agent_observability_access",
    }


def test_schema_backed_resources_have_explicit_creation_order() -> None:
    resources = bundle()["resources"]

    for resource in (
        resources["jobs"]["setup_unity_governance"],
        resources["model_services"]["agent_model"],
        resources["experiments"]["agent_observability"],
    ):
        assert resource["lifecycle"]["depends_on"] == [
            {"resource": "schemas.project_schema"}
        ]


def test_governance_reconciliation_uses_serverless_compute() -> None:
    job = bundle()["resources"]["jobs"]["reconcile_governance"]
    task = job["tasks"][0]

    assert "job_clusters" not in job
    assert "libraries" not in task
    assert task["environment_key"] == "governance"
    assert job["environments"] == [
        {
            "environment_key": "governance",
            "spec": {
                "client": "4",
                "dependencies": ["databricks-sdk==0.142.0", "httpx==0.28.1"],
            },
        }
    ]
    parameters = task["spark_python_task"]["parameters"]
    assert parameters[::2] == [
        "--catalog",
        "--schema",
        "--app-name",
        "--skill-id",
        "--connection-id",
        "--service-id",
        "--policy-function-name",
        "--uc-audience",
    ]


def test_continuous_rtm_waits_for_the_app_owned_lakebase_schema() -> None:
    rtm = bundle()["resources"]["jobs"]["telemetry_rtm"]

    assert rtm["continuous"]["pause_status"] == "UNPAUSED"
    assert rtm["lifecycle"]["depends_on"] == [{"resource": "apps.command_center"}]
    assert rtm["run_as"]["user_name"] == "${workspace.current_user.userName}"
    assert "migrate_lakebase_schema" not in bundle()["resources"]["jobs"]
    assert "setup_runtime_principal_access" not in bundle()["resources"]["jobs"]


def test_runtime_jobs_use_the_public_source_at_the_bundle_commit() -> None:
    jobs = bundle()["resources"]["jobs"]
    for key in (
        "telemetry_rtm",
        "evaluate_agent_traces",
        "reconcile_governance",
    ):
        source = jobs[key]["git_source"]
        assert source == {
            "git_url": (
                "https://github.com/dan-keeling-databricks/"
                "asset-reliability-command-center.git"
            ),
            "git_provider": "gitHub",
            "git_commit": "${bundle.git.commit}",
        }


def test_legacy_python_deployment_wrappers_are_absent() -> None:
    assert not (ROOT / "scripts" / "deploy.py").exists()
    assert not (ROOT / "scripts" / "deploy_data_plane.py").exists()
    assert not (ROOT / "app.yaml.template").exists()


def test_customer_target_is_the_single_production_target() -> None:
    targets = bundle()["targets"]

    assert targets["customer"]["mode"] == "production"
    assert "prod" not in targets

