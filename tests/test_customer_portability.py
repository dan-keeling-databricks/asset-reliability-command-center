"""Customer portability contracts for source and installation documentation."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ORIGINAL_ENVIRONMENT_MARKERS = (
    "fe_vm_rtm",
    "fevm-classic-stable-realtime-demos.cloud.databricks.com",
    "7474659563748021",
    "bd5178cb-8774-4d77-bc63-b4f491716d0b",
    "2162b643ec0f802a",
    "classic_stable_realtime_demos_catalog",
)

DEPLOYABLE_FILES = (
    ROOT / "databricks.yml",
    ROOT / "scripts/build_app.sh",
    ROOT / "scripts/deploy_governance.py",
)


def test_deployable_assets_do_not_reference_the_original_workspace() -> None:
    sources = "\n".join(path.read_text(encoding="utf-8") for path in DEPLOYABLE_FILES)

    for marker in ORIGINAL_ENVIRONMENT_MARKERS:
        assert marker not in sources


def test_customer_guide_is_short_and_centred_on_bundle_deploy() -> None:
    guide = (ROOT / "docs/CUSTOMER_DEPLOYMENT.md").read_text(encoding="utf-8")

    assert len(guide.splitlines()) < 100
    assert "databricks bundle deploy --target customer" in guide
    assert "BUNDLE_VAR_catalog" in guide
    assert "BUNDLE_VAR_schema" in guide
    assert "BUNDLE_VAR_warehouse_id" in guide
    assert "BUNDLE_VAR_workload_node_type" in guide
    assert "preflight" not in guide.casefold()
    assert "rollback" not in guide.casefold()
    assert "decommission" not in guide.casefold()


def test_customer_values_example_contains_only_required_bundle_values() -> None:
    example = (ROOT / "docs/examples/customer-values.example.yml").read_text(
        encoding="utf-8"
    )

    assert [
        line.split(":", maxsplit=1)[0]
        for line in example.splitlines()
        if line and not line.startswith("#")
    ] == ["catalog", "schema", "warehouse_id", "workload_node_type"]


def test_customer_target_is_production_mode() -> None:
    bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")

    assert "  customer:\n    mode: production" in bundle
    assert "root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/" in bundle


def test_readme_exposes_the_single_command_installation() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "databricks bundle deploy --target customer" in readme
    assert "docs/CUSTOMER_DEPLOYMENT.md" in readme
    assert "scripts/deploy.py" not in readme

