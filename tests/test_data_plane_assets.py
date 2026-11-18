"""Static contracts for the governed telemetry pipeline and Genie definition."""

from __future__ import annotations

import json
import re
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import deploy_data_plane


def deployment_args(**overrides) -> Namespace:
    values = {
        "profile": "explicit-profile",
        "target": "dev",
        "access_group": "existing-group",
        "app_service_principal_client_id": "existing-app-sp",
        "catalog": "catalog",
        "schema": "schema",
        "volume": "runtime",
        "warehouse_id": "warehouse-123",
        "workload_node_type": "Standard_D8s_v5",
        "workspace_host": "https://workspace.example",
        "app_name": "customer-app",
        "lakebase_project_id": "customer-project",
        "lakebase_branch": "main",
        "lakebase_database": "customer-database",
        "lakebase_database_resource_name": "customer-database-resource",
        "lakebase_endpoint_name": "primary",
        "lakebase_schema": "asset_reliability",
        "foundation_model": "system.ai.customer-model",
        "model_service_name": "agent-model",
        "mlflow_experiment_name": "/Shared/customer-agent",
        "mlflow_trace_catalog": "catalog",
        "mlflow_trace_schema": "agent_traces",
        "mlflow_trace_table_prefix": "customer_agent",
        "mcp_service_name": "asset_reliability_mcp",
        "governed_skill_name": "asset-reliability-response",
        "policy_function_name": "submit_work_order_policy",
        "global_agent_concurrency": 20,
        "sandbox_capacity": 40,
        "telemetry_samples_per_second": 2,
        "telemetry_source_partitions": 4,
        "timeout_seconds": 60,
        "poll_seconds": 0,
    }
    values.update(overrides)
    return Namespace(**values)

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "pipelines" / "governed_telemetry_history.sql"
RATE_RTM = ROOT / "src" / "streaming" / "rate_to_lakebase_rtm.py"
KAFKA_RTM_REFERENCE = ROOT / "src" / "streaming" / "kafka_to_lakebase_rtm.py"
LAKEBASE_MIGRATION = ROOT / "src" / "migrations" / "apply_lakebase_schema.py"
GENIE = ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json.template"
PRODUCTION_REFERENCE = ROOT / "resources" / "production_reference.yml"


def test_pipeline_is_documented_sql_with_required_datasets() -> None:
    source = PIPELINE.read_text()

    assert source.startswith("-- Databricks notebook source")
    assert "STREAM read_kafka(" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_bronze" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_silver" in source
    assert "CREATE OR REFRESH STREAMING TABLE telemetry_quarantine" in source
    assert "CREATE VIEW pump_telemetry_history" in source
    assert source.count("CONSTRAINT ") >= 9

    # Every executable notebook cell must be introduced by a Markdown cell. This keeps the
    # pipeline readable as text in the repository and after workspace deployment.
    cells = source.split("-- COMMAND ----------")
    executable_cells = [cell for cell in cells if re.search(r"\bCREATE\b", cell)]
    for cell in executable_cells:
        cell_index = cells.index(cell)
        assert cell_index > 0
        assert "-- MAGIC %md" in cells[cell_index - 1]


def test_curated_view_excludes_control_and_broker_fields() -> None:
    source = PIPELINE.read_text()
    view_sql = source.split("CREATE VIEW pump_telemetry_history", maxsplit=1)[1]

    for excluded in (
        "scenario_id",
        "sequence",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ):
        assert excluded not in view_sql

    for included in (
        "event_time",
        "pump_id",
        "flow_m3h",
        "vibration_mm_s",
        "bearing_temperature_c",
        "motor_current_a",
    ):
        assert included in view_sql


def test_genie_is_limited_to_the_curated_telemetry_view() -> None:
    definition = json.loads(GENIE.read_text())
    tables = definition["data_sources"]["tables"]

    assert len(tables) == 1
    assert tables[0]["identifier"] == "{{catalog}}.{{schema}}.pump_telemetry_history"
    assert [item["column_name"] for item in tables[0]["column_configs"]] == sorted(
        item["column_name"] for item in tables[0]["column_configs"]
    )

    serialized = json.dumps(definition).lower()
    for excluded_concept in ("scenario", "restart", "incident", "work order"):
        assert excluded_concept not in serialized


def test_genie_id_keyed_arrays_are_sorted_and_well_formed() -> None:
    definition = json.loads(GENIE.read_text())
    collections = (
        definition["config"]["sample_questions"],
        definition["instructions"]["example_question_sqls"],
        definition["instructions"]["text_instructions"],
    )

    all_ids: list[str] = []
    for collection in collections:
        ids = [item["id"] for item in collection]
        assert ids == sorted(ids)
        assert all(re.fullmatch(r"[0-9a-f]{32}", item_id) for item_id in ids)
        all_ids.extend(ids)
    assert len(all_ids) == len(set(all_ids))


def test_genie_v2_uses_only_v2_column_assistance_fields() -> None:
    definition = json.loads(GENIE.read_text())
    assert definition["version"] == 2

    columns = definition["data_sources"]["tables"][0]["column_configs"]
    serialized = json.dumps(columns)
    assert "get_example_values" not in serialized
    assert "build_value_dictionary" not in serialized

    pump_id = next(item for item in columns if item["column_name"] == "pump_id")
    assert pump_id["enable_format_assistance"] is True
    assert pump_id["enable_entity_matching"] is True


def test_genie_formats_numeric_answers_to_one_decimal_place() -> None:
    definition = json.loads(GENIE.read_text())
    instructions = "\n".join(
        line
        for item in definition["instructions"]["text_instructions"]
        for line in item["content"]
    )
    example_sql = "\n".join(
        sql
        for item in definition["instructions"]["example_question_sqls"]
        for sql in item["sql"]
    )

    assert "one decimal place" in instructions
    assert "Do not alter pump identifiers or timestamps" in instructions
    assert "ROUND(" in example_sql


def test_bundle_uses_classic_pipeline_native_genie_and_obo_scope() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    production_reference = PRODUCTION_REFERENCE.read_text()
    combined_resources = bundle + production_reference
    app = (ROOT / "app.yaml.template").read_text()

    assert "governed_telemetry_history:" not in bundle
    assert "governed_telemetry_history:" in production_reference
    assert 'databricks_cli_version: ">=1.17.0"' in bundle
    assert "serverless: false" in production_reference
    assert "continuous: true" not in combined_resources
    assert "governed_telemetry_history_continuous:" in production_reference
    assert (
        "pipeline_id: ${resources.pipelines.governed_telemetry_history.id}"
        in production_reference
    )
    assert combined_resources.count("pause_status: UNPAUSED") == 1
    assert production_reference.count("pause_status: PAUSED") == 2
    assert "level: CAN_RUN" in production_reference
    assert "node_type_id: ${var.telemetry_pipeline_node_type}" in production_reference
    assert "fleet_telemetry_analyst:" in production_reference
    assert "default: /Shared" in production_reference
    assert combined_resources.count(
        "service_principal_name: ${resources.apps.command_center.service_principal_client_id}"
    ) == 9
    assert "asset_reliability_workloads:" in bundle
    assert "project_schema:" in bundle
    assert '"minValue": 0' in bundle
    assert '"maxValue": 1' in bundle
    assert '"value": "${var.workload_node_type}"' in bundle
    assert "aws_attributes" not in combined_resources
    assert "i3.xlarge" not in combined_resources
    # Lakeflow Declarative Pipelines reject policies that define data_security_mode. The four
    # standalone job clusters still set SINGLE_USER explicitly in their resource definitions.
    assert '"data_security_mode": {' not in bundle
    assert combined_resources.count("data_security_mode: SINGLE_USER") == 4
    assert bundle.count("node_type_id: ${var.workload_node_type}") == 3
    assert production_reference.count(
        "node_type_id: ${var.telemetry_pipeline_node_type}"
    ) == 2
    assert "runtime_engine: STANDARD" in bundle
    assert "trigger_interval: 10 seconds" in bundle
    assert combined_resources.count(
        "policy_id: ${resources.cluster_policies.asset_reliability_workloads.id}"
    ) == 5
    assert "- genie" not in bundle
    assert "dashboards.genie" not in bundle
    assert "DATABRICKS_GENIE_SPACE_ID" not in app
    assert "GENIE_SOURCE_LABEL" not in app
    assert "name: GLOBAL_AGENT_CONCURRENCY" in app
    assert 'value: "{{global_agent_concurrency}}"' in app
    assert "dan.keeling@" not in app


def test_rate_rtm_is_the_only_active_synthetic_source() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    source = RATE_RTM.read_text()
    active_job = bundle.split("    telemetry_rtm:", maxsplit=1)[1].split(
        "    evaluate_agent_traces:", maxsplit=1
    )[0]

    assert RATE_RTM.is_file()
    assert KAFKA_RTM_REFERENCE.is_file()
    assert "notebook_path: ./src/streaming/rate_to_lakebase_rtm.py" in active_job
    assert 'spark.readStream.format("rate")' in source
    assert '.option("rowsPerSecond", ROWS_PER_SECOND)' in source
    assert '.option("numPartitions", SOURCE_PARTITIONS)' in source
    assert 'F.lit("spark-rate-demo").alias("source")' in source
    assert "generator_slot" in source
    assert "pump_index" in source
    assert "sequence" in source
    assert "pmod" in source
    assert "floor" in source
    assert 'format("postgresql")' in source
    assert '.outputMode("update")' in source
    assert '.option("batchinterval", "100 milliseconds")' in source
    assert "/Volumes/" in source
    assert ".trigger(realTime=" in source
    assert 'format("kafka")' not in source
    assert "kafka_bootstrap" not in active_job
    assert "kafka_topic" not in active_job
    assert "p202_pulse" not in source
    assert "p207_pulse" not in source
    assert "def pulse(" not in source


def test_normal_transients_are_one_shot_per_personal_scenario() -> None:
    """Keep P-202/P-207 replay explicit instead of periodic in the shared Rate stream."""

    schema = (ROOT / "sql" / "lakebase_schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS asset_reliability.normal_transient_events" in schema
    assert "PRIMARY KEY (scenario_id, pump_id)" in schema
    assert "status IN ('pending', 'active', 'completed')" in schema
    assert "FOR UPDATE" in schema
    assert "SET status = 'active', started_at = transient_started_at" in schema
    assert "SET status = 'completed', completed_at = NEW.event_time" in schema
    assert "NEW.pump_id = 'P-202'" in schema
    assert "NEW.pump_id = 'P-207'" in schema
    assert "POWER(SIN(PI()" in schema


def test_rate_rtm_notebook_markdown_and_python_cells_are_well_formed() -> None:
    source = RATE_RTM.read_text()
    cells = source.split("# COMMAND ----------")

    assert cells[0].startswith("# Databricks notebook source\n# MAGIC %md\n")
    for index, raw_cell in enumerate(cells):
        cell = raw_cell.removeprefix("# Databricks notebook source\n")
        nonblank_lines = [line for line in cell.splitlines() if line.strip()]
        if not nonblank_lines:
            continue
        if any(line.startswith("# MAGIC %md") for line in nonblank_lines):
            assert nonblank_lines[0].startswith("# MAGIC %md"), (
                f"Markdown cell {index} must begin with %md"
            )
            assert all(line.startswith("# MAGIC") for line in nonblank_lines), (
                f"Markdown cell {index} contains unprefixed content"
            )
            continue
        compile(cell, str(RATE_RTM), "exec")


def test_app_admission_uses_broad_group_without_product_roles() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    app_resource = bundle.split("    command_center:", maxsplit=1)[1].split(
        "targets:", maxsplit=1
    )[0]

    assert "app_access_group:" in bundle
    app_access_variable = bundle.split("  app_access_group:", maxsplit=1)[1].split(
        "  lakebase_project_id:", maxsplit=1
    )[0]
    assert "default:" not in app_access_variable
    assert "level: CAN_USE" in app_resource
    assert "group_name: ${var.app_access_group}" in app_resource
    before_apps = bundle.split("  apps:", maxsplit=1)[0]
    assert before_apps.count("group_name: ${var.app_access_group}") == 1
    assert "experiments:" in before_apps
    assert "level: CAN_READ" in before_apps.split("  experiments:", maxsplit=1)[1]


def test_lakebase_migration_runs_as_app_sp_and_verifies_runtime_contract() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    source = LAKEBASE_MIGRATION.read_text()

    migration_resource = bundle.split("    migrate_lakebase_schema:", maxsplit=1)[1].split(
        "    telemetry_producer:", maxsplit=1
    )[0]
    assert (
        "service_principal_name: "
        "${resources.apps.command_center.service_principal_client_id}"
    ) in migration_resource
    assert "notebook_path: ./src/migrations/apply_lakebase_schema.py" in migration_resource
    assert "lakebase_schema: ${var.lakebase_schema}" in migration_resource
    assert "sandbox_capacity: ${var.sandbox_capacity}" in migration_resource
    assert 'workspace_path.parents[2] / "sql" / "lakebase_schema.sql"' in source
    assert 'dbutils.widgets.text("sandbox_capacity", "32")' in source
    assert "REQUIRED_TABLES" in source
    assert '"agent_run_contexts"' in source
    assert "generate_database_credential" in source
    assert "information_schema.columns" in source
    for required_column in (
        "lease_expires_at",
        "claim_version",
        "attempt_count",
        "approval_claim_id",
        "approval_claim_version",
    ):
        assert required_column in source


def test_access_grant_exposes_only_the_curated_view() -> None:
    grants = (ROOT / "sql" / "grant_telemetry_history.sql").read_text()

    assert "GRANT USE CATALOG" in grants
    assert "GRANT USE SCHEMA" in grants
    assert "GRANT SELECT" in grants
    assert "telemetry_view_fqn" in grants
    assert "telemetry_bronze" not in grants
    assert "telemetry_silver" not in grants
    assert "telemetry_quarantine" not in grants


def test_runtime_principal_grant_is_minimal_and_idempotent() -> None:
    grants = (ROOT / "sql" / "grant_runtime_principal.sql").read_text()

    assert "GRANT USE CATALOG" in grants
    assert "GRANT USE SCHEMA" in grants
    assert "GRANT CREATE TABLE" in grants
    assert "GRANT READ VOLUME" in grants
    assert "GRANT WRITE VOLUME" in grants
    assert "system.information_schema.catalog_privileges" in grants
    assert "system.information_schema.schema_privileges" in grants
    assert "system.information_schema.volume_privileges" in grants
    assert "CREATE USER" not in grants
    assert "CREATE GROUP" not in grants
    assert "ALL PRIVILEGES" not in grants


def test_deployment_helper_migrates_and_starts_rate_without_optional_reference_jobs(
    monkeypatch,
) -> None:
    events: list[tuple[object, ...]] = []
    args = deployment_args()

    monkeypatch.setattr(
        deploy_data_plane,
        "selective_deploy",
        lambda _args, *resources: events.append(("select", *resources)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "run",
        lambda command, **_kwargs: events.append(("run", *command)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "verify_compute_policy_access",
        lambda _args: events.append(("verify_policy",)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "set_continuous_pause_status",
        lambda _args, key, status: events.append(("pause_status", key, status)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "stop_continuous_job",
        lambda _args, key: events.append(("stop", key)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "grant_bundle_source_read",
        lambda _args: events.append(("grant_source_read",)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "wait_for_current_rate_task",
        lambda _args, **_kwargs: events.append(("wait_rate",))
        or (
            {"run_id": 123, "start_time": 456},
            {
                "task_key": "ingest_pump_telemetry",
                "run_id": 124,
                "state": {"life_cycle_state": "RUNNING"},
            },
        ),
    )

    deploy_data_plane.deploy(args)

    labels = [event[0] for event in events]
    assert labels == [
        "select",
        "run",
        "verify_policy",
        "grant_source_read",
        "run",
        "run",
        "run",
        "grant_source_read",
        "pause_status",
        "stop",
        "pause_status",
        "wait_rate",
    ]
    assert events[0] == (
        "select",
        "volumes.runtime_volume",
        "cluster_policies.asset_reliability_workloads",
        "jobs.setup_runtime_principal_access",
        "jobs.migrate_lakebase_schema",
    )
    assert "migrate_lakebase_schema" in events[4]
    assert events[5][:7] == (
        "run",
        "databricks",
        "bundle",
        "deploy",
        "--target",
        "dev",
        "--profile",
    )
    assert "app_access_group=existing-group" in events[5]
    assert "catalog=catalog" in events[5]
    assert "lakebase_branch_name=main" in events[5]
    assert "lakebase_database_name=customer-database" in events[5]
    assert "lakebase_database_resource_name=customer-database-resource" in events[5]
    assert "lakebase_endpoint_name=primary" in events[5]
    assert "--auto-approve" in events[5]
    assert "setup_agent_observability_access" in events[6]
    assert events[8:] == [
        ("pause_status", "telemetry_rtm", "PAUSED"),
        ("stop", "telemetry_rtm"),
        ("pause_status", "telemetry_rtm", "UNPAUSED"),
        ("wait_rate",),
    ]


def test_migration_failure_prevents_every_continuous_restart(monkeypatch) -> None:
    args = deployment_args()
    replacements: list[str] = []

    monkeypatch.setattr(deploy_data_plane, "selective_deploy", lambda *_args: None)
    monkeypatch.setattr(deploy_data_plane, "verify_compute_policy_access", lambda _args: None)
    monkeypatch.setattr(deploy_data_plane, "grant_bundle_source_read", lambda _args: None)
    monkeypatch.setattr(
        deploy_data_plane,
        "set_continuous_pause_status",
        lambda _args, key, _status: replacements.append(key),
    )

    def fail_migration(command, **_kwargs):
        if deploy_data_plane.LAKEBASE_MIGRATION_KEY in command:
            raise RuntimeError("migration failed")
        return None

    monkeypatch.setattr(deploy_data_plane, "run", fail_migration)

    with pytest.raises(RuntimeError, match="migration failed"):
        deploy_data_plane.deploy(args)
    assert replacements == []


def test_bundle_source_read_is_derived_granted_and_verified(monkeypatch) -> None:
    args = Namespace(
        profile="explicit-profile",
        target="dev",
        access_group="existing-group",
        app_service_principal_client_id="existing-app-sp",
        catalog="catalog",
        schema="schema",
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(
        deploy_data_plane,
        "bundle_summary",
        lambda _args: {"workspace": {"file_path": "/Workspace/bundle/dev/files"}},
    )

    def fake_run_json(command: list[str]) -> dict[str, object]:
        commands.append(command)
        if "get-status" in command:
            return {"object_id": 123, "path": "/Workspace/bundle/dev/files"}
        if "update-permissions" in command:
            return {}
        return {
            "access_control_list": [
                {
                    "service_principal_name": "existing-app-sp",
                    "all_permissions": [{"permission_level": "CAN_READ"}],
                }
            ]
        }

    monkeypatch.setattr(deploy_data_plane, "run_json", fake_run_json)

    deploy_data_plane.grant_bundle_source_read(args)

    assert commands[0][1:4] == ["workspace", "get-status", "/Workspace/bundle/dev/files"]
    assert commands[1][1:5] == [
        "workspace",
        "update-permissions",
        "directories",
        "123",
    ]
    payload = json.loads(commands[1][commands[1].index("--json") + 1])
    assert payload == {
        "access_control_list": [
            {
                "permission_level": "CAN_READ",
                "service_principal_name": "existing-app-sp",
            }
        ]
    }
    assert commands[2][1:5] == [
        "workspace",
        "get-permissions",
        "directories",
        "123",
    ]


def test_running_parent_with_pending_rate_task_does_not_pass_readiness(monkeypatch) -> None:
    args = Namespace(
        profile="explicit-profile",
        timeout_seconds=1,
        poll_seconds=0,
    )
    parent_summary = {
        "run_id": 2,
        "start_time": 100_000,
        "state": {"life_cycle_state": "RUNNING"},
    }
    get_run_commands: list[list[str]] = []
    get_run_responses = iter(
        (
            {
                **parent_summary,
                "tasks": [
                    {
                        "task_key": "ingest_pump_telemetry",
                        "run_id": 20,
                        "state": {
                            "life_cycle_state": "PENDING",
                            "state_message": "MAX_CONCURRENT_RUNS_REACHED",
                        },
                    }
                ],
            },
            {
                **parent_summary,
                "tasks": [
                    {
                        "task_key": "ingest_pump_telemetry",
                        "run_id": 21,
                        "state": {"life_cycle_state": "RUNNING"},
                    }
                ],
            },
        )
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "active_job_runs",
        lambda _args, _key: [parent_summary],
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "run_json",
        lambda command: get_run_commands.append(command) or next(get_run_responses),
    )

    parent, task = deploy_data_plane.wait_for_current_rate_task(
        args,
        restart_requested_at_ms=100_000,
    )

    assert parent["run_id"] == 2
    assert task["run_id"] == 21
    assert len(get_run_commands) == 2
    assert get_run_commands[0] == [
        "databricks",
        "jobs",
        "get-run",
        "2",
        "--output",
        "json",
        "--profile",
        "explicit-profile",
    ]


def test_active_job_runs_accepts_cli_top_level_array(monkeypatch) -> None:
    args = Namespace(profile="explicit-profile")
    monkeypatch.setattr(deploy_data_plane, "resource_id", lambda *_args: "123")
    monkeypatch.setattr(
        deploy_data_plane,
        "run",
        lambda command, capture: Namespace(
            stdout=json.dumps(
                [{"run_id": 42, "state": {"life_cycle_state": "RUNNING"}}]
            )
        ),
    )

    assert deploy_data_plane.active_job_runs(args, "telemetry_producer") == [
        {"run_id": 42, "state": {"life_cycle_state": "RUNNING"}}
    ]


def test_continuous_pause_update_uses_explicit_profile(monkeypatch) -> None:
    args = Namespace(profile="explicit-profile")
    commands: list[list[str]] = []
    monkeypatch.setattr(deploy_data_plane, "resource_id", lambda *_args: "123")
    monkeypatch.setattr(
        deploy_data_plane,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )

    deploy_data_plane.set_continuous_pause_status(
        args, deploy_data_plane.RATE_JOB_KEY, "PAUSED"
    )

    assert commands == [
        [
            "databricks",
            "jobs",
            "update",
            "--json",
            (
                '{"job_id": 123, "new_settings": '
                '{"continuous": {"pause_status": "PAUSED"}}}'
            ),
            "--profile",
            "explicit-profile",
        ]
    ]


def test_obsolete_continuous_job_cancels_each_parent_explicitly(monkeypatch) -> None:
    args = Namespace(profile="explicit-profile", timeout_seconds=1, poll_seconds=0)
    commands: list[list[str]] = []
    active_responses = iter([[{"run_id": 42}], []])
    monkeypatch.setattr(deploy_data_plane, "resource_id", lambda *_args: "123")
    monkeypatch.setattr(
        deploy_data_plane,
        "active_job_runs",
        lambda _args, _key: next(active_responses),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )

    deploy_data_plane.stop_continuous_job(args, "telemetry_producer")

    assert commands[0][1:4] == ["jobs", "cancel-all-runs", "--job-id"]
    assert commands[1][1:5] == ["jobs", "cancel-run", "42", "--no-wait"]
