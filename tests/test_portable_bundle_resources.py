"""Portability contracts for the DAB-first customer installation."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = ROOT / "databricks.yml"
PRODUCTION_REFERENCE_PATH = ROOT / "resources" / "production_reference.yml"
GENIE_TEMPLATE_PATH = ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json.template"


def _bundle() -> dict:
    return yaml.safe_load(BUNDLE_PATH.read_text(encoding="utf-8"))


def test_only_customer_owned_workspace_choices_are_required() -> None:
    variables = _bundle()["variables"]
    required = {name for name, config in variables.items() if "default" not in config}

    assert required == {"catalog", "schema", "warehouse_id", "workload_node_type"}
    assert "workspace_host" not in variables
    assert "app_service_principal_client_id" not in variables


def test_app_identity_and_lakebase_paths_are_derived_by_the_bundle() -> None:
    source = BUNDLE_PATH.read_text(encoding="utf-8")

    assert "name: ${var.app_name}" in source
    assert "${resources.apps.command_center.service_principal_client_id}" in source
    assert "${var.app_service_principal_client_id}" not in source
    assert "branches/production" in source
    assert "databases/databricks-postgres" in source
    assert "endpoints/primary" in source
    assert "lakebase_database: databricks_postgres" in source
    assert "lakebase_schema: ${var.lakebase_schema}" in source


def test_app_runtime_configuration_is_parameterised_in_the_bundle() -> None:
    app = _bundle()["resources"]["apps"]["command_center"]
    environment = {item["name"]: item for item in app["config"]["env"]}

    assert environment["LAKEBASE_ENDPOINT"]["value_from"] == "lakebase"
    assert environment["MODEL_ENDPOINT"]["value_from"] == "agent-model-service"
    assert environment["MLFLOW_EXPERIMENT_ID"]["value_from"] == (
        "agent-mlflow-experiment"
    )
    assert environment["GOVERNED_MCP_ENDPOINT"]["value"].startswith(
        "${workspace.host}/ai-gateway/mcp-services/"
    )
    assert environment["GOVERNED_SKILL_TOOL"]["value"].startswith("skill_${var.catalog}")
    assert not (ROOT / "app.yaml.template").exists()


def test_workspace_profile_and_original_demo_ids_are_not_committed() -> None:
    sources = "\n".join(
        [
            BUNDLE_PATH.read_text(encoding="utf-8"),
            PRODUCTION_REFERENCE_PATH.read_text(encoding="utf-8"),
            GENIE_TEMPLATE_PATH.read_text(encoding="utf-8"),
            (ROOT / "sql/setup_demo.sql").read_text(encoding="utf-8"),
            (ROOT / "sql/grant_runtime_principal.sql").read_text(encoding="utf-8"),
        ]
    )

    for environment_value in (
        "fe_vm_rtm",
        "fevm-classic-stable-realtime-demos",
        "classic_stable_realtime_demos_catalog",
        "2162b643ec0f802a",
        "bd5178cb-8774-4d77-bc63-b4f491716d0b",
        "63.186.81.193",
    ):
        assert environment_value not in sources


def test_sql_grants_and_policy_use_parameterised_identifiers() -> None:
    runtime_grants = (ROOT / "sql/grant_runtime_principal.sql").read_text()
    policy = (ROOT / "sql/setup_demo.sql").read_text()

    assert ":app_service_principal" in runtime_grants
    assert runtime_grants.count("EXECUTE IMMEDIATE") == 5
    assert policy.count("IDENTIFIER(:policy_function_fqn)") == 2


def test_production_reference_remains_parameterised_and_opt_in() -> None:
    primary = BUNDLE_PATH.read_text(encoding="utf-8")
    optional = PRODUCTION_REFERENCE_PATH.read_text(encoding="utf-8")
    genie = GENIE_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "include" not in _bundle()
    assert "production_reference.yml" not in primary
    assert "kafka_bootstrap:" not in primary
    assert "genie_spaces:" not in primary
    assert "kafka_bootstrap:" in optional
    assert optional.count("pause_status: PAUSED") == 2
    assert "{{catalog}}.{{schema}}.pump_telemetry_history" in genie
