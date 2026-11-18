"""MLflow observability configuration tests that do not require workspace credentials."""

import mlflow
import mlflow.langchain

from backend.observability import agent_observability, configure_mlflow_tracing
from backend.settings import Settings


def test_observability_links_use_injected_workspace_and_experiment() -> None:
    settings = Settings(
        _env_file=None,
        databricks_host="workspace.example",
        mlflow_experiment_id="123456789",
    )

    result = agent_observability(settings)

    assert result.framework == "LangGraph"
    assert result.status == "configured"
    assert result.experiment_url == "https://workspace.example/ml/experiments/123456789"
    assert result.traces_url == (
        "https://workspace.example/ml/experiments/123456789/traces"
    )


def test_mlflow_configuration_is_optional_for_local_development() -> None:
    settings = Settings(_env_file=None, databricks_host="https://workspace.example")

    result = configure_mlflow_tracing(settings)

    assert result.status == "unavailable"
    assert result.experiment_url is None
    assert "not configured" in (result.detail or "")


def test_mlflow_configuration_reuses_the_exact_uc_table_prefix(monkeypatch) -> None:
    configured: dict[str, object] = {}
    monkeypatch.setattr(mlflow, "set_tracking_uri", lambda uri: configured.update(uri=uri))
    monkeypatch.setattr(
        mlflow,
        "set_experiment",
        lambda **kwargs: configured.update(kwargs),
    )
    monkeypatch.setattr(mlflow.langchain, "autolog", lambda **_kwargs: None)
    settings = Settings(
        _env_file=None,
        databricks_host="https://workspace.example",
        mlflow_experiment_id="123456789",
        mlflow_tracing_destination="demo_catalog.agent_traces",
        mlflow_trace_table_prefix="customer_agent",
        mlflow_tracing_sql_warehouse_id="warehouse-123",
    )

    result = configure_mlflow_tracing(settings)

    location = configured["trace_location"]
    assert result.status == "configured"
    assert configured["uri"] == "databricks"
    assert configured["experiment_id"] == "123456789"
    assert location.catalog_name == "demo_catalog"
    assert location.schema_name == "agent_traces"
    assert location.table_prefix == "customer_agent"
