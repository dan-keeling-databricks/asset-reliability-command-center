"""Environment-backed configuration for local and deployed App runtimes."""

from functools import lru_cache
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _workspace_host(host: str | None) -> str | None:
    """Return the Apps-injected workspace host as a normalized HTTPS origin."""

    normalized_host = (host or "").rstrip("/")
    if not normalized_host:
        return None
    if not normalized_host.startswith(("http://", "https://")):
        normalized_host = f"https://{normalized_host}"
    return normalized_host


def _workspace_url(host: str | None, value: str | None) -> str | None:
    """Resolve a workspace-relative App setting without embedding a deployment hostname."""

    if not value or value.startswith(("http://", "https://")):
        return value
    normalized_host = _workspace_host(host)
    if not normalized_host:
        return value
    return f"{normalized_host}/{value.lstrip('/')}"


class Settings(BaseSettings):
    """Validated runtime settings populated from App resources and environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_adapter: Literal["local", "lakebase"] = "local"
    telemetry_interval_seconds: float = 1.0
    alert_evaluator_interval_seconds: float = 1.0
    local_event_step_seconds: float = 0.3
    lakebase_dsn: str | None = None
    lakebase_schema: str = "asset_reliability"
    lakebase_job_principal: str | None = None
    sse_coalesce_seconds: float = 0.05
    telemetry_retention_seconds: int = 86_400
    telemetry_retention_interval_seconds: float = 300.0
    telemetry_retention_batch_size: int = 1_000
    sandbox_ttl_seconds: int = 7_200
    global_agent_concurrency: int = 20
    databricks_host: str | None = None
    model_endpoint: str = "databricks-claude-sonnet-4-5"
    mlflow_experiment_id: str | None = None
    mlflow_tracing_destination: str | None = None
    mlflow_trace_table_prefix: str | None = None
    mlflow_tracing_sql_warehouse_id: str | None = None
    governed_mcp_endpoint: str | None = None
    governed_skill_endpoint: str | None = None
    governed_skill_tool: str | None = None
    governed_skill_document_url: str | None = None
    approval_poll_seconds: float = 2.0
    model_input_cost_per_million: float = 3.0
    model_output_cost_per_million: float = 15.0

    @model_validator(mode="after")
    def resolve_workspace_urls(self) -> "Settings":
        """Use the Apps-injected workspace host for portable Gateway and Skill paths."""

        self.databricks_host = _workspace_host(self.databricks_host)
        for field in (
            "governed_mcp_endpoint",
            "governed_skill_endpoint",
            "governed_skill_document_url",
        ):
            setattr(self, field, _workspace_url(self.databricks_host, getattr(self, field)))
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
