"""Configure MLflow tracing and expose safe links to its managed experiment."""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass

from backend.settings import Settings

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentObservability:
    """User-facing observability configuration without credentials or inferred traces."""

    framework: str
    experiment_id: str | None
    experiment_url: str | None
    traces_url: str | None
    status: str
    detail: str | None

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


def configure_mlflow_tracing(settings: Settings) -> AgentObservability:
    """Enable LangGraph autologging against the bundle-managed MLflow experiment.

    The App service identity supplies Databricks authentication. Configuration failures are
    surfaced in the Agent graph tab but do not disable live asset monitoring.
    """

    observability = agent_observability(settings)
    if not settings.mlflow_experiment_id:
        return observability

    try:
        import mlflow
        import mlflow.langchain
        from mlflow.entities.trace_location import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        destination = settings.mlflow_tracing_destination
        if destination:
            catalog, separator, schema = destination.partition(".")
            if not separator or not catalog or not schema or "." in schema:
                raise ValueError("MLFLOW_TRACING_DESTINATION must be a catalog.schema name")
            table_prefix = settings.mlflow_trace_table_prefix
            if not table_prefix:
                raise ValueError(
                    "MLFLOW_TRACE_TABLE_PREFIX is required for a Unity Catalog trace location"
                )
            if settings.mlflow_tracing_sql_warehouse_id:
                os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = (
                    settings.mlflow_tracing_sql_warehouse_id
                )
            mlflow.set_experiment(
                experiment_id=settings.mlflow_experiment_id,
                trace_location=UnityCatalog(
                    catalog_name=catalog,
                    schema_name=schema,
                    table_prefix=table_prefix,
                ),
            )
        else:
            mlflow.set_experiment(experiment_id=settings.mlflow_experiment_id)
        mlflow.langchain.autolog(log_traces=True, silent=True)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("MLflow LangGraph tracing could not be configured")
        return AgentObservability(
            framework="LangGraph",
            experiment_id=settings.mlflow_experiment_id,
            experiment_url=observability.experiment_url,
            traces_url=observability.traces_url,
            status="unavailable",
            detail=f"MLflow tracing configuration failed: {type(exc).__name__}",
        )
    return observability


def agent_observability(settings: Settings) -> AgentObservability:
    """Build links only from the injected experiment ID and authenticated workspace host."""

    experiment_id = settings.mlflow_experiment_id
    host = (settings.databricks_host or "").rstrip("/")
    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    experiment_url = (
        f"{host}/ml/experiments/{experiment_id}" if host and experiment_id else None
    )
    configured = bool(experiment_id and experiment_url)
    return AgentObservability(
        framework="LangGraph",
        experiment_id=experiment_id,
        experiment_url=experiment_url,
        # The experiment is the verified stable entry point for its Traces view. A more
        # specific URL is returned only after a persisted trace ID has been read back.
        traces_url=experiment_url,
        status="configured" if configured else "unavailable",
        detail=(
            "Open the managed experiment and select Traces to inspect runtime execution."
            if configured
            else "MLflow experiment resources are not configured in this environment."
        ),
    )
