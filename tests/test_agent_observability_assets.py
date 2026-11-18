"""Validate governed model inference and MLflow observability bundle assets."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_app_uses_unity_gateway_model_service_and_managed_experiment() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    app = (ROOT / "app.yaml.template").read_text()

    assert "model_services:" in bundle
    assert "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL" in bundle
    assert "model: models/${var.foundation_model}" in bundle
    assert "securable_type: MODEL_SERVICE" in bundle
    assert "permission: EXECUTE" in bundle
    assert "serving_endpoint:" not in bundle.split("  apps:", maxsplit=1)[1]
    assert "valueFrom: agent-model-service" in app

    assert "experiments:" in bundle
    assert "uc_trace_location:" in bundle
    assert "table_prefix: ${var.mlflow_trace_table_prefix}" in bundle
    assert 'value: "{{mlflow_trace_table_prefix}}"' in app
    assert "permission: CAN_EDIT" in bundle
    assert "valueFrom: agent-mlflow-experiment" in app
    assert "valueFrom: sql-warehouse" in app


def test_observability_grants_are_explicit_and_least_privilege() -> None:
    grants = (ROOT / "sql" / "grant_agent_observability.sql").read_text()

    for suffix in ("spans", "logs", "metrics", "annotations"):
        assert f"_otel_{suffix}" in grants
    assert "GRANT MODIFY, SELECT ON TABLE" in grants
    assert "GRANT SELECT ON TABLE" in grants
    assert ":trace_reader_principal" in grants
    assert ":app_access_group" not in grants
    assert "ALL PRIVILEGES" not in grants
    assert "CREATE USER" not in grants
    assert "CREATE GROUP" not in grants


def test_evaluation_uses_real_traces_and_mlflow_genai_evaluate() -> None:
    notebook = (ROOT / "src" / "evaluation" / "evaluate_agent_traces.py").read_text()
    bundle = (ROOT / "databricks.yml").read_text()

    assert '"P-202": "no_action"' in notebook
    assert '"P-207": "no_action"' in notebook
    assert '"P-204": "schedule_work_order"' in notebook
    assert "mlflow.search_traces(" in notebook
    assert "mlflow.genai.evaluate(" in notebook
    assert "ExpectationsGuidelines()" in notebook
    assert "predict_fn=" not in notebook
    assert "missing " in notebook
    assert "mlflow[databricks]>=3.18,<4" in bundle
