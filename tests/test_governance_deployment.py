"""Deployment reconciliation tests for the governed MCP service."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.service.catalog import ProvisioningInfoState

import scripts.deploy_governance as deployment
from backend.gateway import GatewayAsk, GatewayElicitation
from scripts.deploy_governance import (
    TOOLS,
    GovernanceConfig,
    _connection_drift,
    _service_config_matches,
    ensure_connection,
    ensure_service,
    ensure_user_permissions,
    probe_submit_policy,
    resolve_skill_directory,
)

ROOT = Path(__file__).resolve().parents[1]


def config() -> GovernanceConfig:
    return GovernanceConfig(
        profile="customer-profile",
        catalog="customer_catalog",
        schema="asset_reliability",
        app_name="customer-reliability-app",
        skill_id="asset-reliability-response",
        connection_id="asset_reliability_mcp_connection",
        service_id="asset_reliability_mcp",
        policy_function_name="submit_work_order_policy",
        uc_audience="customer-users",
    )


def test_skill_source_resolves_without_module_file(monkeypatch) -> None:
    monkeypatch.delitem(deployment.__dict__, "__file__", raising=False)
    monkeypatch.chdir(ROOT)

    assert resolve_skill_directory("asset-reliability-response") == (
        ROOT / "skills/asset-reliability-response"
    )


def test_governance_configuration_has_no_maintainer_defaults(monkeypatch) -> None:
    variables = {
        "DATABRICKS_CONFIG_PROFILE": "customer-profile",
        "ASSET_RELIABILITY_CATALOG": "customer_catalog",
        "ASSET_RELIABILITY_SCHEMA": "asset_reliability",
        "ASSET_RELIABILITY_APP_NAME": "customer-app",
        "ASSET_RELIABILITY_SKILL_ID": "asset-reliability-response",
        "ASSET_RELIABILITY_MCP_CONNECTION_ID": "customer_connection",
        "ASSET_RELIABILITY_MCP_SERVICE_ID": "customer_service",
        "ASSET_RELIABILITY_POLICY_FUNCTION_NAME": "customer_policy",
        "ASSET_RELIABILITY_UC_AUDIENCE": "customer-users",
    }
    for name in variables:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="Missing explicit governance"):
        GovernanceConfig.from_environment()

    for name, value in variables.items():
        monkeypatch.setenv(name, value)
    resolved = GovernanceConfig.from_environment()
    assert resolved.profile == "customer-profile"
    assert resolved.service_fqn == "customer_catalog.asset_reliability.customer_service"


def test_user_permissions_cover_skill_and_gateway_without_genie_data() -> None:
    updates: list[tuple[str, str, list]] = []

    class Grants:
        def update(self, securable_type, full_name, *, changes):
            updates.append((securable_type, full_name, changes))

    settings = config()
    applied = ensure_user_permissions(SimpleNamespace(grants=Grants()), settings)

    assert [item[:2] for item in updates] == [
        ("CATALOG", settings.catalog),
        ("SCHEMA", f"{settings.catalog}.{settings.schema}"),
        ("SKILL", settings.skill_fqn),
        ("MCP_SERVICE", settings.service_fqn),
    ]
    assert all(item[2][0].principal == "customer-users" for item in updates)
    assert [item[2][0].add[0].value for item in updates] == [
        "USE_CATALOG",
        "USE_SCHEMA",
        "READ_VOLUME",
        "EXECUTE",
    ]
    assert len(applied) == 4


def test_connection_drift_ignores_redacted_secret_but_detects_routing_change() -> None:
    connection = SimpleNamespace(
        options={"host": "https://app.example", "base_path": "/mcp", "client_id": "app-sp"}
    )

    assert not _connection_drift(
        connection,
        {
            "host": "https://app.example",
            "base_path": "/mcp",
            "client_id": "app-sp",
            "client_secret": "<redacted>",
        },
    )
    assert _connection_drift(
        connection,
        {
            "host": "https://different.example",
            "base_path": "/mcp",
            "client_id": "app-sp",
            "client_secret": "<redacted>",
        },
    )


def test_failed_connection_rotates_credential_even_without_visible_drift() -> None:
    updated_options: list[dict[str, str]] = []
    created_secrets: list[str] = []

    app = SimpleNamespace(
        url="https://app.example",
        service_principal_client_id="app-sp",
        service_principal_id=123,
    )
    workspace_config = SimpleNamespace(host="https://workspace.example")
    desired = deployment._connection_options(
        SimpleNamespace(config=workspace_config), app, "<redacted>"
    )
    connection = SimpleNamespace(
        options={key: value for key, value in desired.items() if key != "client_secret"},
        provisioning_info=SimpleNamespace(state=ProvisioningInfoState.FAILED),
    )

    class Connections:
        def get(self, _name):
            return connection

        def update(self, _name, options):
            updated_options.append(options)
            return SimpleNamespace(
                options=options,
                provisioning_info=SimpleNamespace(state=ProvisioningInfoState.ACTIVE),
            )

    class Secrets:
        def create(self, _principal_id, lifetime):
            assert lifetime == "31536000s"
            created_secrets.append("created")
            return SimpleNamespace(secret="fresh-secret", id="secret-id")

        def delete(self, _principal_id, _secret_id):
            raise AssertionError("Successful rotation must not delete the new credential")

    client = SimpleNamespace(
        config=workspace_config,
        connections=Connections(),
        service_principal_secrets_proxy=Secrets(),
    )

    reconciled, status = ensure_connection(client, app, config())

    assert status == "recovered"
    assert created_secrets == ["created"]
    assert updated_options == [
        deployment._connection_options(client, app, "fresh-secret")
    ]
    assert reconciled.provisioning_info.state == ProvisioningInfoState.ACTIVE


def test_new_connection_retries_transient_oauth_credential_propagation(monkeypatch) -> None:
    attempts: list[dict[str, str]] = []
    sleeps: list[float] = []

    app = SimpleNamespace(
        url="https://app.example",
        service_principal_client_id="app-sp",
        service_principal_id=123,
    )

    class Connections:
        def get(self, _name):
            raise deployment.NotFound("missing")

        def create(self, **kwargs):
            attempts.append(kwargs)
            if len(attempts) < 3:
                raise RuntimeError(
                    "OAuth token exchange failed: invalid_request while credential propagates"
                )
            return SimpleNamespace(full_name="connection")

    class Secrets:
        def create(self, _principal_id, lifetime):
            assert lifetime == "31536000s"
            return SimpleNamespace(secret="fresh-secret", id="secret-id")

        def delete(self, _principal_id, _secret_id):
            raise AssertionError("Successful retry must retain its credential")

    client = SimpleNamespace(
        config=SimpleNamespace(host="https://workspace.example"),
        connections=Connections(),
        service_principal_secrets_proxy=Secrets(),
    )
    monkeypatch.setattr(deployment.time, "sleep", sleeps.append)

    connection, status = ensure_connection(client, app, config())

    assert connection.full_name == "connection"
    assert status == "created"
    assert len(attempts) == 3
    assert sleeps == [5.0, 10.0]


def test_service_drift_is_patched_to_declared_tools_and_ask_policy() -> None:
    calls: list[tuple[str, str, dict | None, dict | None]] = []
    current = {"include_tool_selectors": ["stale"]}

    class ApiClient:
        def do(self, method, path, body=None, query=None):
            nonlocal current
            calls.append((method, path, body, query))
            if method == "GET":
                return {"name": "service", "config": current}
            current = body["config"]
            return {"name": "reconciled", "config": body["config"]}

    service, status = ensure_service(
        SimpleNamespace(api_client=ApiClient()),
        "policy-function-id",
        config(),
        retry_delay_seconds=0,
    )

    assert status == "updated"
    assert service["config"]["include_tool_selectors"] == TOOLS
    assert service["config"]["service_policies"] == [
        {
            "name": "submit_work_order_requires_approval",
            "policy_type": "POLICY_TYPE_CUSTOM",
            "handler": f"functions/{config().policy_fqn}",
            "function_id": "policy-function-id",
            "rank": 1,
        }
    ]
    assert [call[0] for call in calls] == ["GET", "PATCH", "GET"]
    assert calls[1][3] == {"update_mask": "config"}


def test_service_match_ignores_gateway_expansion_but_not_governed_drift() -> None:
    desired = deployment._service_config("policy-function-id", config())
    actual = {
        "source_connection": {
            "name": "connections/asset_reliability_mcp_connection",
            "id": "connection-id",
            "status": "ACTIVE",
        },
        "include_tool_selectors": TOOLS,
        "service_policies": [
            {
                **desired["service_policies"][0],
                "is_deleted": False,
            }
        ],
    }

    assert _service_config_matches(actual, desired)
    actual["include_tool_selectors"] = [*TOOLS, "unexpected_tool"]
    assert not _service_config_matches(actual, desired)


def test_service_patch_is_retried_until_a_fresh_get_observes_declared_config() -> None:
    calls: list[str] = []
    desired = deployment._service_config("policy-function-id", config())
    get_count = 0

    class ApiClient:
        def do(self, method, _path, body=None, query=None):
            nonlocal get_count
            calls.append(method)
            if method == "PATCH":
                assert query == {"update_mask": "config"}
            if method == "GET":
                get_count += 1
                if get_count < 3:
                    return {"name": "stale", "config": {"include_tool_selectors": ["stale"]}}
                return {"name": "ready", "config": desired}
            return {"name": "patch-accepted", "config": body["config"]}

    service, status = ensure_service(
        SimpleNamespace(api_client=ApiClient()),
        "policy-function-id",
        config(),
        max_attempts=3,
        retry_delay_seconds=0,
    )

    assert status == "updated"
    assert service["name"] == "ready"
    assert calls == ["GET", "PATCH", "GET", "PATCH", "GET"]


@pytest.mark.asyncio
async def test_policy_probe_discovers_tools_rejects_unscoped_source_then_gets_ask(
    monkeypatch,
) -> None:
    events: list[str] = []

    class FakeGateway:
        def __init__(self, endpoint):
            assert endpoint.endswith(config().service_fqn)

        async def list_tools(self, token):
            assert token == "obo-token"
            events.append("list")
            return [{"name": name} for name in TOOLS]

        async def call_tool(self, name, arguments, token):
            assert token == "obo-token"
            events.append(name)
            if name == "get_asset_telemetry":
                assert arguments == {"asset_id": "P-204"}
                raise RuntimeError("Missing internal agent run context")
            raise GatewayAsk(
                '{"method":"tools/call"}',
                GatewayElicitation("ask-1", "https://example.test/ask", "Approval required"),
            )

        async def close(self):
            events.append("close")

    monkeypatch.setattr(deployment, "GovernedGatewayClient", FakeGateway)
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://workspace.example",
            authenticate=lambda: {"Authorization": "Bearer obo-token"},
        )
    )

    result = await probe_submit_policy(client, config(), retry_delay_seconds=0)

    assert events == ["list", "get_asset_telemetry", "submit_work_order", "close"]
    assert result == {
        "tools": TOOLS,
        "source_context_guard": "verified",
        "decision": "ASK",
        "elicitation_id": "ask-1",
        "execution": "not executed",
    }
    assert "obo-token" not in str(result)


@pytest.mark.asyncio
async def test_policy_probe_rejects_any_tool_surface_drift(monkeypatch) -> None:
    class FakeGateway:
        def __init__(self, _endpoint):
            pass

        async def list_tools(self, _token):
            return [{"name": name} for name in [*TOOLS, "unexpected_tool"]]

        async def close(self):
            pass

    monkeypatch.setattr(deployment, "GovernedGatewayClient", FakeGateway)
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://workspace.example",
            authenticate=lambda: {"Authorization": "Bearer obo-token"},
        )
    )

    with pytest.raises(RuntimeError, match="exact declared tool set"):
        await probe_submit_policy(client, config(), max_attempts=1, retry_delay_seconds=0)
