"""Deploy the Unity Gateway skill, App-backed MCP connection, service, and ASK policy."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel
from databricks.sdk.service.catalog import (
    ConnectionType,
    PermissionsChange,
    Privilege,
    ProvisioningInfoState,
    SecurableType,
)

from backend.gateway import GatewayAsk, GovernedGatewayClient

TOOLS = [
    "get_asset_telemetry",
    "assess_failure_risk",
    "prepare_work_order",
    "submit_work_order",
]


@dataclass(frozen=True)
class GovernanceConfig:
    """Explicit, customer-owned names for governed deployment resources."""

    profile: str | None
    catalog: str
    schema: str
    app_name: str
    skill_id: str
    connection_id: str
    service_id: str
    policy_function_name: str
    uc_audience: str

    @property
    def parent(self) -> str:
        return f"schemas/{self.catalog}.{self.schema}"

    @property
    def skill_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.skill_id}"

    @property
    def connection_fqn(self) -> str:
        # MCP source connections are workspace/metastore-root connections.
        return self.connection_id

    @property
    def service_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.service_id}"

    @property
    def policy_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.policy_function_name}"

    @classmethod
    def from_environment(cls) -> GovernanceConfig:
        names = {
            "catalog": "ASSET_RELIABILITY_CATALOG",
            "schema": "ASSET_RELIABILITY_SCHEMA",
            "app_name": "ASSET_RELIABILITY_APP_NAME",
            "skill_id": "ASSET_RELIABILITY_SKILL_ID",
            "connection_id": "ASSET_RELIABILITY_MCP_CONNECTION_ID",
            "service_id": "ASSET_RELIABILITY_MCP_SERVICE_ID",
            "policy_function_name": "ASSET_RELIABILITY_POLICY_FUNCTION_NAME",
            "uc_audience": "ASSET_RELIABILITY_UC_AUDIENCE",
        }
        values = {field: os.getenv(variable, "").strip() for field, variable in names.items()}
        missing = [names[field] for field, value in values.items() if not value]
        if missing:
            raise RuntimeError(
                "Missing explicit governance deployment settings: " + ", ".join(missing)
            )
        return cls(
            profile=os.getenv("DATABRICKS_CONFIG_PROFILE", "").strip() or None,
            **values,
        )


def resolve_skill_directory(skill_id: str) -> Path:
    """Locate Skill source in either a normal module or a Git-backed Spark task."""

    roots = [Path.cwd(), *Path.cwd().parents]
    module_file = globals().get("__file__")
    if module_file:
        roots.insert(0, Path(str(module_file)).resolve().parents[1])
    for root in dict.fromkeys(roots):
        candidate = root / "skills" / skill_id
        if (candidate / "SKILL.md").is_file():
            return candidate
    raise RuntimeError(f"Unable to locate governed Skill source for {skill_id!r}")


def ensure_source_app_permission(
    client: WorkspaceClient, app, config: GovernanceConfig
) -> bool:
    """Allow the connection's M2M principal through the Databricks App boundary."""

    permissions = client.apps.get_permissions(config.app_name)
    for entry in permissions.access_control_list or []:
        if entry.service_principal_name != app.service_principal_client_id:
            continue
        levels = {permission.permission_level for permission in entry.all_permissions or []}
        if AppPermissionLevel.CAN_USE in levels or AppPermissionLevel.CAN_MANAGE in levels:
            return False

    client.apps.update_permissions(
        config.app_name,
        access_control_list=[
            AppAccessControlRequest(
                service_principal_name=app.service_principal_client_id,
                permission_level=AppPermissionLevel.CAN_USE,
            )
        ],
    )
    return True


def deploy_skill(client: WorkspaceClient, config: GovernanceConfig) -> dict[str, object]:
    """Create the governed Skill if needed, upload all source files, and finalize it."""

    try:
        client.api_client.do("GET", f"/api/2.1/unity-catalog/skills/{config.skill_fqn}")
        created = False
    except NotFound:
        client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/skills",
            query={"parent": config.parent, "skill_id": config.skill_id},
            body={"comment": "Evidence-led response procedure for autonomous asset anomalies"},
        )
        created = True

    skill_directory = resolve_skill_directory(config.skill_id)
    uploaded: list[str] = []
    for local_path in skill_directory.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(skill_directory).as_posix()
        client.files.upload(
            f"/Skills/{config.catalog}/{config.schema}/{config.skill_id}/{relative_path}",
            BytesIO(local_path.read_bytes()),
            overwrite=True,
        )
        uploaded.append(relative_path)

    finalized = client.api_client.do(
        "POST",
        f"/api/2.1/unity-catalog/skills/{config.skill_fqn}/finalize",
        body={},
    )
    return {"created": created, "uploaded": uploaded, "name": finalized.get("name")}


def _connection_options(client: WorkspaceClient, app, client_secret: str) -> dict[str, str]:
    parsed = urlparse(app.url)
    return {
        "host": f"{parsed.scheme}://{parsed.netloc}",
        "port": "443",
        "base_path": "/mcp",
        "client_id": app.service_principal_client_id,
        "client_secret": client_secret,
        "token_endpoint": f"{client.config.host}/oidc/v1/token",
        "oauth_scope": "all-apis",
        "is_mcp_connection": "true",
    }


def _connection_drift(connection, desired: dict[str, str]) -> bool:
    """Compare only non-secret options because GET redacts the OAuth client secret."""

    current = connection.options or {}
    return any(
        current.get(key) != value for key, value in desired.items() if key != "client_secret"
    )


def _connection_provisioning_failed(connection) -> bool:
    """Return whether Gateway can no longer authenticate through the connection.

    HTTP OAuth connections can retain the same visible routing options after their stored
    client credential stops refreshing access tokens. Treat the platform's ``FAILED`` state as
    credential drift so reconciliation rotates the secret instead of preserving a broken
    connection indefinitely.
    """

    provisioning_info = getattr(connection, "provisioning_info", None)
    return getattr(provisioning_info, "state", None) == ProvisioningInfoState.FAILED


def ensure_connection(
    client: WorkspaceClient, app, config: GovernanceConfig
) -> tuple[object, str]:
    """Create or reconcile the App-backed MCP source connection."""

    try:
        connection = client.connections.get(config.connection_fqn)
    except NotFound:
        # The generated secret is passed directly to the connection API and is never logged or
        # persisted in the source tree.
        secret = client.service_principal_secrets_proxy.create(
            str(app.service_principal_id), lifetime="31536000s"
        )
        try:
            connection = client.connections.create(
                name=config.connection_id,
                connection_type=ConnectionType.HTTP,
                comment="App-hosted maintenance MCP tools for Asset Reliability",
                read_only=False,
                options=_connection_options(client, app, secret.secret),
            )
        except Exception:
            # Avoid leaving an unreferenced App-SP credential if connection creation fails.
            client.service_principal_secrets_proxy.delete(str(app.service_principal_id), secret.id)
            raise
        return connection, "created"

    desired_without_secret = _connection_options(client, app, "<redacted>")
    connection_failed = _connection_provisioning_failed(connection)
    if not connection_failed and not _connection_drift(connection, desired_without_secret):
        return connection, "unchanged"
    secret = client.service_principal_secrets_proxy.create(
        str(app.service_principal_id), lifetime="31536000s"
    )
    try:
        connection = client.connections.update(
            config.connection_fqn,
            _connection_options(client, app, secret.secret),
        )
    except Exception:
        client.service_principal_secrets_proxy.delete(str(app.service_principal_id), secret.id)
        raise
    return connection, "recovered" if connection_failed else "updated"


def _service_config(function_id: str, config: GovernanceConfig) -> dict[str, object]:
    return {
        "source_connection": {"name": f"connections/{config.connection_fqn}"},
        "include_tool_selectors": TOOLS,
        "service_policies": [
            {
                "name": "submit_work_order_requires_approval",
                "policy_type": "POLICY_TYPE_CUSTOM",
                "handler": f"functions/{config.policy_fqn}",
                "function_id": function_id,
                "rank": 1,
            }
        ],
    }


def _service_config_matches(actual: dict[str, object], desired: dict[str, object]) -> bool:
    """Compare governed inputs while ignoring read-only fields expanded by Gateway GET."""

    actual_source = actual.get("source_connection") or {}
    desired_source = desired.get("source_connection") or {}
    if not isinstance(actual_source, dict) or not isinstance(desired_source, dict):
        return False
    if actual_source.get("name") != desired_source.get("name"):
        return False
    if actual.get("include_tool_selectors") != desired.get("include_tool_selectors"):
        return False

    governed_policy_fields = (
        "name",
        "policy_type",
        "handler",
        "function_id",
        "rank",
    )

    def project_policies(config: dict[str, object]) -> list[dict[str, object]]:
        policies = config.get("service_policies") or []
        if not isinstance(policies, list):
            return []
        return [
            {field: policy.get(field) for field in governed_policy_fields}
            for policy in policies
            if isinstance(policy, dict)
        ]

    return project_policies(actual) == project_policies(desired)


def ensure_service(
    client: WorkspaceClient,
    function_id: str,
    config: GovernanceConfig,
    *,
    max_attempts: int = 6,
    retry_delay_seconds: float = 0.5,
) -> tuple[dict, str]:
    """Create or reconcile the service, verifying PATCH propagation with fresh GETs."""

    desired = _service_config(function_id, config)

    try:
        service = client.api_client.do(
            "GET", f"/api/2.1/unity-catalog/mcp-services/{config.service_fqn}"
        )
    except NotFound:
        service = client.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/mcp-services",
            query={"parent": config.parent, "mcp_service_id": config.service_id},
            body={
                "comment": "Governed tools for the Asset Reliability Command Center",
                "config": desired,
            },
        )
        return service, "created"
    if _service_config_matches(service.get("config") or {}, desired):
        return service, "unchanged"

    for attempt in range(max_attempts):
        client.api_client.do(
            "PATCH",
            f"/api/2.1/unity-catalog/mcp-services/{config.service_fqn}",
            query={"update_mask": "config"},
            body={"config": desired},
        )
        service = client.api_client.do(
            "GET", f"/api/2.1/unity-catalog/mcp-services/{config.service_fqn}"
        )
        if _service_config_matches(service.get("config") or {}, desired):
            return service, "updated"
        if attempt + 1 < max_attempts:
            time.sleep(retry_delay_seconds * (2**attempt))
    raise RuntimeError("Unity Gateway service did not converge to the declared configuration")


def ensure_user_permissions(
    client: WorkspaceClient, config: GovernanceConfig
) -> list[str]:
    """Grant the App's broad audience only the UC privileges needed by the agent.

    Databricks Apps admits the workspace-local ``users`` group. Unity Catalog uses the special
    ``account users`` principal for the corresponding account-wide audience, while the App ACL
    remains the narrower workspace admission boundary.
    """

    grants = (
        (SecurableType.CATALOG, config.catalog, Privilege.USE_CATALOG),
        (SecurableType.SCHEMA, f"{config.catalog}.{config.schema}", Privilege.USE_SCHEMA),
        (SecurableType.SKILL, config.skill_fqn, Privilege.READ_VOLUME),
        (SecurableType.MCP_SERVICE, config.service_fqn, Privilege.EXECUTE),
    )
    applied: list[str] = []
    for securable_type, full_name, privilege in grants:
        client.grants.update(
            securable_type.value,
            full_name,
            changes=[PermissionsChange(principal=config.uc_audience, add=[privilege])],
        )
        applied.append(f"{securable_type.value}:{full_name}:{privilege.value}")
    return applied


async def probe_submit_policy(
    client: WorkspaceClient,
    config: GovernanceConfig,
    *,
    max_attempts: int = 6,
    retry_delay_seconds: float = 0.5,
) -> dict[str, object]:
    """Prove discovery, source authentication, and native ASK after propagation.

    The result is intentionally sanitized: neither the OBO token, source headers, nor raw
    telemetry are returned or logged.
    """

    authorization = client.config.authenticate().get("Authorization", "")
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise RuntimeError("Unable to obtain an OAuth token for the policy probe")
    gateway_url = f"{client.config.host}/ai-gateway/mcp-services/{config.service_fqn}"
    gateway = GovernedGatewayClient(gateway_url)
    try:
        for attempt in range(max_attempts):
            try:
                tools = await gateway.list_tools(token)
                discovered = [str(tool.get("name", "")) for tool in tools]
                if len(discovered) != len(TOOLS) or set(discovered) != set(TOOLS):
                    raise RuntimeError(
                        "Gateway tool discovery did not match the exact declared tool set"
                    )

                try:
                    await gateway.call_tool(
                        "get_asset_telemetry", {"asset_id": "P-204"}, token
                    )
                except RuntimeError as exc:
                    if "run context" not in str(exc).casefold():
                        raise RuntimeError("Unexpected source context-guard failure") from exc
                else:
                    raise RuntimeError("Context-free source call was not rejected")

                try:
                    await gateway.call_tool(
                        "submit_work_order",
                        {
                            "asset_id": "P-204",
                            "action": "Policy deployment probe; must not execute",
                            "incident_id": "00000000-0000-0000-0000-000000000000",
                            "planned_window": "never",
                        },
                        token,
                    )
                except GatewayAsk as ask:
                    return {
                        "tools": discovered,
                        "source_context_guard": "verified",
                        "decision": "ASK",
                        "elicitation_id": ask.elicitation.elicitation_id,
                        "execution": "not executed",
                    }
                raise RuntimeError("submit_work_order policy probe did not return native ASK")
            except Exception:
                if attempt + 1 >= max_attempts:
                    raise
                await asyncio.sleep(retry_delay_seconds * (2**attempt))
    finally:
        await gateway.close()
    raise RuntimeError("Unity Gateway deployment probe exhausted without success")


def main() -> None:
    """Deploy governance dependencies in their required dependency order."""

    config = GovernanceConfig.from_environment()
    client = WorkspaceClient(profile=config.profile) if config.profile else WorkspaceClient()
    app = client.apps.get(config.app_name)
    if not app.url or not app.service_principal_id or not app.service_principal_client_id:
        raise RuntimeError("Deploy and start the Databricks App before configuring governance")

    function = client.functions.get(config.policy_fqn)
    app_permission_created = ensure_source_app_permission(client, app, config)
    skill = deploy_skill(client, config)
    connection, connection_status = ensure_connection(client, app, config)
    service, service_status = ensure_service(client, function.function_id, config)
    user_permissions = ensure_user_permissions(client, config)
    policy_probe = asyncio.run(probe_submit_policy(client, config))

    print(
        json.dumps(
            {
                "skill": skill,
                "source_app_permission_created": app_permission_created,
                "connection": connection.full_name,
                "connection_status": connection_status,
                "service": service.get("name"),
                "service_status": service_status,
                "user_permissions": user_permissions,
                "gateway_url": (
                    f"{client.config.host}/ai-gateway/mcp-services/{config.service_fqn}"
                ),
                "policy": config.policy_fqn,
                "tools": TOOLS,
                "policy_probe": policy_probe,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
