"""MLflow observability configuration tests that do not require workspace credentials."""

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
    assert result.traces_url == result.experiment_url


def test_mlflow_configuration_is_optional_for_local_development() -> None:
    settings = Settings(_env_file=None, databricks_host="https://workspace.example")

    result = configure_mlflow_tracing(settings)

    assert result.status == "unavailable"
    assert result.experiment_url is None
    assert "not configured" in (result.detail or "")
