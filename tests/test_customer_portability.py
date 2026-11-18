"""Customer portability contracts for deployable source and lifecycle documentation."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# These values identify the maintainer's original demonstration workspace. They may remain in
# dated execution evidence and explicitly labelled documentation examples, but never in source
# that can be bundled or staged into a customer deployment.
ORIGINAL_ENVIRONMENT_MARKERS = {
    "CLI profile": "fe_vm_rtm",
    "workspace host": "fevm-classic-stable-realtime-demos.cloud.databricks.com",
    "workspace organisation": "7474659563748021",
    "App service principal": "bd5178cb-8774-4d77-bc63-b4f491716d0b",
    "reference group principal": "84285700224068",
    "reference group": "asset-reliability-command-center-operator",
    "SQL warehouse": "2162b643ec0f802a",
    "Kafka broker": "63.186.81.193",
    "Unity Catalog catalog": "classic_stable_realtime_demos_catalog",
    "Unity Catalog schema": "asset_reliability_command_center",
}

DEPLOYABLE_FILES = {
    Path("app.yaml"),
    Path("app.yaml.template"),
    Path("databricks.yml"),
    Path("pyproject.toml"),
    Path("requirements.txt"),
    Path("frontend/index.html"),
    Path("frontend/package.json"),
    Path("frontend/package-lock.json"),
    Path("frontend/vite.config.ts"),
}
DEPLOYABLE_TREES = (
    Path("backend"),
    Path("frontend/public"),
    Path("frontend/src"),
    Path("genie"),
    Path("resources"),
    Path("skills"),
    Path("sql"),
    Path("src"),
)
TEXT_SUFFIXES = {
    ".css",
    ".html",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".template",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}

# Product-adjacent historical records are deliberately retained beside the relevant source. They
# are evidence, not inputs to the App or bundle, and each file labels the captured run as dated.
DATED_EVIDENCE_ALLOWLIST = {
    Path("backend/AUTONOMOUS_AGENT_WORK_ORDER_EXECUTION_LOG.md"),
    Path("backend/UNITY_GATEWAY_HUMAN_APPROVAL_EXECUTION_LOG.md"),
    Path("frontend/DEPLOYED_DATABRICKS_APP_API_RESPONSES.md"),
    Path("genie/GENIE_CONVERSATION_GENERATED_SQL_AND_RESULTS.md"),
    Path("sql/UNITY_CATALOG_AND_LAKEBASE_QUERY_RESULTS.md"),
    Path("src/pipelines/LAKEFLOW_PIPELINE_RUN_OUTPUT_AND_ROW_COUNTS.md"),
}


def deployable_text_files() -> list[Path]:
    """Return repository-relative text assets that can enter a bundle or App build."""

    paths = {path for path in DEPLOYABLE_FILES if (ROOT / path).is_file()}
    for tree in DEPLOYABLE_TREES:
        paths.update(
            path.relative_to(ROOT)
            for path in (ROOT / tree).rglob("*")
            if path.is_file() and path.suffix.lower() in TEXT_SUFFIXES
        )
    return sorted(paths - DATED_EVIDENCE_ALLOWLIST)


def test_deployable_assets_do_not_reference_original_environment() -> None:
    """Prevent a customer bundle from inheriting the maintainer's workspace identifiers."""

    violations: list[str] = []
    for path in deployable_text_files():
        contents = (ROOT / path).read_text(encoding="utf-8")
        for label, marker in ORIGINAL_ENVIRONMENT_MARKERS.items():
            if marker in contents:
                violations.append(f"{path}: original {label} `{marker}`")

    assert not violations, "Original-environment hardcodes found:\n" + "\n".join(violations)


def test_hardcode_audit_includes_optional_production_reference_assets() -> None:
    """Keep inactive Kafka, Lakeflow and Genie definitions portable as well as the active path."""

    audited = set(deployable_text_files())
    assert any(
        path in audited
        for path in (
            Path("genie/fleet_telemetry_analyst.geniespace.json"),
            Path("genie/fleet_telemetry_analyst.geniespace.json.template"),
        )
    )
    assert {
        Path("src/producer/pump_telemetry_producer.py"),
        Path("src/pipelines/governed_telemetry_history.sql"),
        Path("src/streaming/kafka_to_lakebase_rtm.py"),
    } <= audited


def test_customer_lifecycle_guide_covers_safe_operating_boundaries() -> None:
    """Keep customer installation, updates and rollback discoverable and explicit."""

    guide = (ROOT / "docs/CUSTOMER_DEPLOYMENT.md").read_text(encoding="utf-8")
    required_phrases = (
        "First installation",
        "Updating an installation",
        "App-only updates",
        "Rollback",
        "Decommissioning",
        "Spark Rate",
        "personal-sandbox",
        "optional production-reference",
        "Kafka",
        "Lakeflow",
        "Genie",
        "Git operations alone never change a workspace",
    )
    assert all(phrase in guide for phrase in required_phrases)


def test_customer_values_example_is_complete_and_explicitly_non_executable() -> None:
    """Provide one target-neutral worksheet without implying that the CLI loads it."""

    example = (ROOT / "docs/examples/customer-values.example.yml").read_text(
        encoding="utf-8"
    )
    required_keys = (
        "profile:",
        "expected_host:",
        "bundle_target:",
        "catalog:",
        "schema:",
        "checkpoint_volume:",
        "sql_warehouse_id:",
        "access_group:",
        "project_id:",
        "branch:",
        "database:",
        "database_resource_name:",
        "endpoint_name:",
        "operational_schema:",
        "source: rate",
        "sandbox_capacity:",
        "foundation_model:",
        "model_service_name:",
        "skill_id:",
        "mcp_connection_id:",
        "mcp_service_id:",
        "policy_function_name:",
        "mlflow_experiment_name:",
        "trace_catalog:",
        "trace_schema:",
        "trace_table_prefix:",
        "production_reference:",
        "enabled: false",
        "bundle_fragment: resources/production_reference.yml",
        "kafka_bootstrap:",
        "production_reference_access_group:",
    )
    assert all(key in example for key in required_keys)
    assert "does not load this file" in example
    assert "Never put secrets" in example


def test_customer_guide_documents_the_complete_installer_contract() -> None:
    """Keep the copyable command aligned with every required customer input."""

    guide = (ROOT / "docs/CUSTOMER_DEPLOYMENT.md").read_text(encoding="utf-8")
    required_options = (
        "--profile",
        "--target",
        "--expected-host",
        "--access-group",
        "--catalog",
        "--schema",
        "--volume",
        "--warehouse-id",
        "--app-name",
        "--lakebase-project-id",
        "--lakebase-branch",
        "--lakebase-database",
        "--lakebase-database-resource-name",
        "--lakebase-endpoint-name",
        "--lakebase-schema",
        "--foundation-model",
        "--model-service-name",
        "--mlflow-experiment-name",
        "--mlflow-trace-catalog",
        "--mlflow-trace-schema",
        "--mlflow-trace-table-prefix",
        "--sandbox-capacity",
        "--telemetry-samples-per-second",
        "--telemetry-source-partitions",
        "--skill-id",
        "--mcp-service-id",
        "--mcp-connection-id",
        "--policy-function-name",
        "--global-agent-concurrency",
    )
    assert all(option in guide for option in required_options)
    assert "`install --app-only` is rejected" in guide


def test_readme_links_customer_installation_guide() -> None:
    """Expose the portable lifecycle guide from the repository's primary entry point."""

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "[Customer installation and lifecycle]" in readme
    assert "docs/CUSTOMER_DEPLOYMENT.md" in readme

