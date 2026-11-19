"""Runtime configuration tests for portable Databricks App workspace URLs."""

from backend.settings import Settings


def test_workspace_relative_governed_urls_use_injected_databricks_host() -> None:
    settings = Settings(
        databricks_host="workspace.example",
        governed_mcp_endpoint="/ai-gateway/mcp-services/catalog.schema.service",
        governed_skill_endpoint="/ai-gateway/skills/?schema=catalog.schema",
        governed_skill_document_url="/api/2.0/fs/files/Skills/catalog/schema/skill/SKILL.md",
    )

    assert settings.databricks_host == "https://workspace.example"
    assert settings.governed_mcp_endpoint == (
        "https://workspace.example/ai-gateway/mcp-services/catalog.schema.service"
    )
    assert settings.governed_skill_endpoint == (
        "https://workspace.example/ai-gateway/skills/?schema=catalog.schema"
    )
    assert settings.governed_skill_document_url == (
        "https://workspace.example/api/2.0/fs/files/Skills/catalog/schema/skill/SKILL.md"
    )


def test_absolute_governed_urls_are_preserved() -> None:
    settings = Settings(
        databricks_host="https://workspace.example/",
        governed_mcp_endpoint="https://gateway.example/mcp",
        governed_skill_endpoint="https://gateway.example/skills",
    )

    assert settings.databricks_host == "https://workspace.example"
    assert settings.governed_mcp_endpoint == "https://gateway.example/mcp"
    assert settings.governed_skill_endpoint == "https://gateway.example/skills"
