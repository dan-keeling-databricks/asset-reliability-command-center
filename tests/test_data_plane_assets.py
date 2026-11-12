"""Static contracts for the governed telemetry pipeline and Genie definition."""

from __future__ import annotations

import json
import re
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import deploy_data_plane

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "pipelines" / "governed_telemetry_history.sql"
RATE_RTM = ROOT / "src" / "streaming" / "rate_to_lakebase_rtm.py"
KAFKA_RTM_REFERENCE = ROOT / "src" / "streaming" / "kafka_to_lakebase_rtm.py"
LAKEBASE_MIGRATION = ROOT / "src" / "migrations" / "apply_lakebase_schema.py"
GENIE = ROOT / "genie" / "fleet_telemetry_analyst.geniespace.json"


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
    assert "${" not in tables[0]["identifier"]
    assert tables[0]["identifier"].endswith(".pump_telemetry_history")
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
    app = (ROOT / "app.yaml").read_text()

    assert "governed_telemetry_history:" in bundle
    assert 'databricks_cli_version: ">=1.17.0"' in bundle
    assert "serverless: false" in bundle
    assert "continuous: true" not in bundle
    assert "governed_telemetry_history_continuous:" in bundle
    assert "pipeline_id: ${resources.pipelines.governed_telemetry_history.id}" in bundle
    assert bundle.count("pause_status: UNPAUSED") == 1
    assert bundle.count("pause_status: PAUSED") == 2
    assert "level: CAN_RUN" in bundle
    assert "node_type_id: ${var.telemetry_pipeline_node_type}" in bundle
    assert "fleet_telemetry_analyst:" in bundle
    assert "default: /Shared" in bundle
    assert "space_id: ${resources.genie_spaces.fleet_telemetry_analyst.id}" in bundle
    assert bundle.count("service_principal_name: ${var.app_service_principal_client_id}") == 7
    assert "asset_reliability_workloads:" in bundle
    assert '"minValue": 0' in bundle
    assert '"maxValue": 1' in bundle
    assert '"value": "i3.xlarge"' in bundle
    assert '"value": "ON_DEMAND"' in bundle
    # Lakeflow Declarative Pipelines reject policies that define data_security_mode. The three
    # standalone job clusters still set SINGLE_USER explicitly in their resource definitions.
    assert '"data_security_mode": {' not in bundle
    assert bundle.count("data_security_mode: SINGLE_USER") == 3
    assert "runtime_engine: STANDARD" in bundle
    assert "trigger_interval: 10 seconds" in bundle
    assert bundle.count(
        "policy_id: ${resources.cluster_policies.asset_reliability_workloads.id}"
    ) == 4
    assert "- genie" in bundle
    assert "dashboards.genie" not in bundle
    assert "DATABRICKS_GENIE_SPACE_ID" in app
    assert "GENIE_SOURCE_LABEL" in app
    assert "valueFrom: fleet-telemetry-genie" in app
    assert "dan.keeling@" not in app


def test_rate_rtm_is_the_only_active_synthetic_source() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    source = RATE_RTM.read_text()
    active_job = bundle.split("    telemetry_rtm:", maxsplit=1)[1].split(
        "    governed_telemetry_history_continuous:", maxsplit=1
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


def test_app_admission_uses_broad_group_without_product_roles() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    app_resource = bundle.split("    command_center:", maxsplit=1)[1].split(
        "targets:", maxsplit=1
    )[0]

    assert "app_access_group:" in bundle
    assert "default: users" in bundle
    assert "level: CAN_USE" in app_resource
    assert "group_name: ${var.app_access_group}" in app_resource
    assert "group_name: ${var.app_access_group}" not in bundle.split(
        "  apps:", maxsplit=1
    )[0]


def test_lakebase_migration_runs_as_app_sp_and_verifies_runtime_contract() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    source = LAKEBASE_MIGRATION.read_text()

    migration_resource = bundle.split("    migrate_lakebase_schema:", maxsplit=1)[1].split(
        "    telemetry_producer:", maxsplit=1
    )[0]
    assert "service_principal_name: ${var.app_service_principal_client_id}" in migration_resource
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
    assert "pump_telemetry_history" in grants
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
    assert grants.count("SHOW GRANTS") == 3
    assert "CREATE USER" not in grants
    assert "CREATE GROUP" not in grants
    assert "ALL PRIVILEGES" not in grants


def test_deployment_helper_migrates_stops_legacy_and_starts_rate(monkeypatch) -> None:
    events: list[tuple[object, ...]] = []
    args = Namespace(
        profile="explicit-profile",
        target="dev",
        access_group="existing-group",
        app_service_principal_client_id="existing-app-sp",
        catalog="catalog",
        schema="schema",
        timeout_seconds=60,
        poll_seconds=0,
    )

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
        "restart_continuous_job",
        lambda _args, key: events.append(("restart", key)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "stop_obsolete_continuous_job",
        lambda _args, key: events.append(("stop", key)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "grant_bundle_source_read",
        lambda _args: events.append(("grant_source_read",)),
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "wait_for_current_rate_run",
        lambda _args, **_kwargs: events.append(("wait_rate",))
        or {"run_id": 123, "start_time": 456},
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "active_job_runs",
        lambda _args, _key: [],
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
        "grant_source_read",
        "stop",
        "stop",
        "restart",
        "wait_rate",
    ]
    assert events[0] == (
        "select",
        "cluster_policies.asset_reliability_workloads",
        "jobs.setup_runtime_principal_access",
        "jobs.migrate_lakebase_schema",
    )
    assert "migrate_lakebase_schema" in events[4]
    assert events[5] == (
        "run",
        "databricks",
        "bundle",
        "deploy",
        "--target",
        "dev",
        "--profile",
        "explicit-profile",
        "--var",
        "app_access_group=existing-group",
        "--var",
        "app_service_principal_client_id=existing-app-sp",
        "--var",
        "catalog=catalog",
        "--var",
        "schema=schema",
        "--auto-approve",
    )
    assert events[7:9] == [
        ("stop", "telemetry_producer"),
        ("stop", "governed_telemetry_history_continuous"),
    ]
    assert events[9:] == [("restart", "telemetry_rtm"), ("wait_rate",)]


def test_migration_failure_prevents_every_continuous_restart(monkeypatch) -> None:
    args = Namespace(
        profile="explicit-profile",
        target="dev",
        access_group="existing-group",
        app_service_principal_client_id="existing-app-sp",
        catalog="catalog",
        schema="schema",
        timeout_seconds=60,
        poll_seconds=0,
    )
    restarted: list[str] = []

    monkeypatch.setattr(deploy_data_plane, "selective_deploy", lambda *_args: None)
    monkeypatch.setattr(deploy_data_plane, "verify_compute_policy_access", lambda _args: None)
    monkeypatch.setattr(deploy_data_plane, "grant_bundle_source_read", lambda _args: None)
    monkeypatch.setattr(
        deploy_data_plane,
        "restart_continuous_job",
        lambda _args, key: restarted.append(key),
    )

    def fail_migration(command, **_kwargs):
        if deploy_data_plane.LAKEBASE_MIGRATION_KEY in command:
            raise RuntimeError("migration failed")
        return None

    monkeypatch.setattr(deploy_data_plane, "run", fail_migration)

    with pytest.raises(RuntimeError, match="migration failed"):
        deploy_data_plane.deploy(args)
    assert restarted == []


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


def test_current_rate_run_must_start_after_restart_request(monkeypatch) -> None:
    args = Namespace(
        profile="explicit-profile",
        timeout_seconds=1,
        poll_seconds=0,
    )
    responses = iter(
        [
            [
                {
                    "run_id": 1,
                    "start_time": 1_000,
                    "state": {"life_cycle_state": "RUNNING"},
                }
            ],
            [
                {
                    "run_id": 2,
                    "start_time": 100_000,
                    "state": {"life_cycle_state": "RUNNING"},
                }
            ],
        ]
    )
    monkeypatch.setattr(
        deploy_data_plane,
        "active_job_runs",
        lambda _args, _key: next(responses),
    )

    run_payload = deploy_data_plane.wait_for_current_rate_run(
        args,
        restart_requested_at_ms=100_000,
        previous_run_ids={"1"},
    )

    assert run_payload["run_id"] == 2


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

    deploy_data_plane.stop_obsolete_continuous_job(args, "telemetry_producer")

    assert commands[0][1:4] == ["jobs", "cancel-all-runs", "--job-id"]
    assert commands[1][1:5] == ["jobs", "cancel-run", "42", "--no-wait"]
