"""Static contracts for the primary Rate-to-Lakebase data plane."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RATE_RTM = ROOT / "src" / "streaming" / "rate_to_lakebase_rtm.py"
LAKEBASE_MIGRATION = ROOT / "src" / "migrations" / "apply_lakebase_schema.py"


def test_rate_rtm_uses_spark_rate_real_time_mode_and_native_postgres_sink() -> None:
    source = RATE_RTM.read_text(encoding="utf-8")

    assert 'spark.readStream.format("rate")' in source
    assert 'telemetry.writeStream.format("postgresql")' in source
    assert ".trigger(realTime=" in source
    assert "checkpointLocation" in source
    assert "/Volumes/" in source


def test_rate_rtm_preserves_personal_sandbox_and_one_shot_transient_contracts() -> None:
    source = RATE_RTM.read_text(encoding="utf-8")
    schema = (ROOT / "sql/lakebase_schema.sql").read_text(encoding="utf-8")

    assert "generator_slot" in source
    assert "scenario_id" in source
    assert "normal_transient_events" in schema
    assert "P-202" in schema
    assert "P-207" in schema


def test_rate_rtm_notebook_cells_are_documented() -> None:
    source = RATE_RTM.read_text(encoding="utf-8")
    cells = source.split("# COMMAND ----------")
    executable = [cell for cell in cells if re.search(r"\bspark\.|writeStream", cell)]

    assert source.startswith("# Databricks notebook source")
    assert executable
    for cell in executable:
        index = cells.index(cell)
        assert index > 0
        assert "# MAGIC %md" in cells[index - 1]


def test_lakebase_migration_is_idempotent_and_contract_checked() -> None:
    source = LAKEBASE_MIGRATION.read_text(encoding="utf-8")

    assert "lakebase_schema.sql" in source
    assert "CREATE SCHEMA IF NOT EXISTS" in (ROOT / "sql/lakebase_schema.sql").read_text()
    assert "REQUIRED_TABLES" in source
    assert "REQUIRED_COLUMNS" in source
    assert "sandbox_capacity" in source
    assert "transaction" in source.casefold()


def test_runtime_principal_grant_remains_least_privilege() -> None:
    grants = (ROOT / "sql/grant_runtime_principal.sql").read_text(encoding="utf-8")

    for privilege in (
        "GRANT USE CATALOG",
        "GRANT USE SCHEMA",
        "GRANT CREATE TABLE",
        "GRANT READ VOLUME",
        "GRANT WRITE VOLUME",
    ):
        assert privilege in grants
    assert "ALL PRIVILEGES" not in grants
