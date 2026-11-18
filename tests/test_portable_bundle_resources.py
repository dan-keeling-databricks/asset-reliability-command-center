"""Contracts that keep bundle resources portable across customer workspaces."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = ROOT / "databricks.yml"
APP_TEMPLATE_PATH = ROOT / "app.yaml.template"
PRODUCTION_REFERENCE_PATH = ROOT / "resources" / "production_reference.yml"
GENIE_TEMPLATE_PATH = ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json.template"


def _bundle() -> dict:
    return yaml.safe_load(BUNDLE_PATH.read_text())


def test_environment_specific_installation_inputs_have_no_committed_defaults() -> None:
    variables = _bundle()["variables"]
    required_inputs = {
        "catalog",
        "schema",
        "workspace_host",
        "app_name",
        "warehouse_id",
        "app_access_group",
        "mlflow_trace_reader_principal",
        "lakebase_project_id",
        "lakebase_branch_name",
        "lakebase_database_name",
        "lakebase_database_resource_name",
        "lakebase_endpoint_name",
        "lakebase_schema",
        "mlflow_trace_catalog",
        "mlflow_trace_schema",
    }

    assert required_inputs <= variables.keys()
    assert all("default" not in variables[name] for name in required_inputs)
    assert "app_service_principal_client_id" not in variables


def test_app_identity_and_resource_paths_are_derived_from_bundle_inputs() -> None:
    source = BUNDLE_PATH.read_text()

    assert "name: ${var.app_name}" in source
    assert "${resources.apps.command_center.service_principal_client_id}" in source
    assert "${var.app_service_principal_client_id}" not in source
    assert "branches/${var.lakebase_branch_name}" in source
    assert "databases/${var.lakebase_database_resource_name}" in source
    assert "endpoints/${var.lakebase_endpoint_name}" in source
    assert "lakebase_database: ${var.lakebase_database_name}" in source
    assert "lakebase_schema: ${var.lakebase_schema}" in source
    assert "branches/production" not in source
    assert "databricks_postgres" not in source


def test_app_manifest_is_an_environment_neutral_render_template() -> None:
    source = APP_TEMPLATE_PATH.read_text()
    variables = _bundle()["variables"]
    template_inputs = set(re.findall(r"\{\{([a-z0-9_]+)\}\}", source))

    assert not (ROOT / "app.yaml").exists()
    assert template_inputs <= variables.keys()
    assert {
        "workspace_host",
        "catalog",
        "schema",
        "mlflow_trace_catalog",
        "mlflow_trace_schema",
        "lakebase_schema",
        "global_agent_concurrency",
    } <= template_inputs
    assert "valueFrom: lakebase" in source
    assert "valueFrom: agent-model-service" in source
    assert "valueFrom: agent-mlflow-experiment" in source
    assert "fevm-classic" not in source
    assert "classic_stable_realtime_demos_catalog" not in source


def test_workspace_profile_and_demo_resource_ids_are_not_committed() -> None:
    sources = "\n".join(
        [
            BUNDLE_PATH.read_text(),
            APP_TEMPLATE_PATH.read_text(),
            PRODUCTION_REFERENCE_PATH.read_text(),
            GENIE_TEMPLATE_PATH.read_text(),
            (ROOT / "sql" / "setup_demo.sql").read_text(),
            (ROOT / "sql" / "grant_runtime_principal.sql").read_text(),
            (ROOT / "sql" / "grant_telemetry_history.sql").read_text(),
        ]
    )

    for environment_value in (
        "fe_vm_rtm",
        "fevm-classic-stable-realtime-demos",
        "classic_stable_realtime_demos_catalog",
        "2162b643ec0f802a",
        "bd5178cb-8774-4d77-bc63-b4f491716d0b",
        "63.186.81.193",
        "asset-reliability-command-center-operator",
    ):
        assert environment_value not in sources


def test_sql_grants_and_policy_function_use_parameterised_identifiers() -> None:
    runtime_grants = (ROOT / "sql" / "grant_runtime_principal.sql").read_text()
    telemetry_grants = (ROOT / "sql" / "grant_telemetry_history.sql").read_text()
    policy = (ROOT / "sql" / "setup_demo.sql").read_text()

    assert ":app_service_principal" in runtime_grants
    assert ":telemetry_analysis_group" in telemetry_grants
    assert runtime_grants.count("EXECUTE IMMEDIATE") == 5
    assert telemetry_grants.count("EXECUTE IMMEDIATE") == 3
    assert policy.count("IDENTIFIER(:policy_function_fqn)") == 2


def test_production_reference_is_parameterised_and_opt_in() -> None:
    primary = BUNDLE_PATH.read_text()
    optional = PRODUCTION_REFERENCE_PATH.read_text()
    genie = GENIE_TEMPLATE_PATH.read_text()

    assert "include" not in _bundle()
    assert "production_reference.yml" not in primary
    assert "kafka_bootstrap:" not in primary
    assert "genie_spaces:" not in primary
    assert "pipelines:" not in primary
    assert "kafka_bootstrap:" in optional
    assert "default:" not in optional.split("  kafka_bootstrap:", maxsplit=1)[1].split(
        "  kafka_topic:", maxsplit=1
    )[0]
    assert "production_reference_access_group:" in optional
    assert optional.count("pause_status: PAUSED") == 2
    assert "{{catalog}}.{{schema}}.pump_telemetry_history" in genie
    assert not (ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json").exists()
