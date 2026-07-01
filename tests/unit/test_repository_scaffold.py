from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_core_docs_exist() -> None:
    expected_files = [
        "README.md",
        ".env.example",
        ".pre-commit-config.yaml",
        "docker-compose.yml",
        "Makefile",
        "pyproject.toml",
        "api/Dockerfile",
        "docs/README.md",
        "docs/architecture.md",
        "docs/implementation-plan.md",
        "docs/stack.md",
        "docs/spec.md",
        "docs/roadmap.md",
        "scripts/print_settings.py",
    ]

    missing = [path for path in expected_files if not (ROOT / path).is_file()]
    assert not missing, f"Missing core docs: {missing}"


def test_required_project_directories_exist() -> None:
    expected_dirs = [
        "api/app",
        "deploy/local/trino/etc",
        "dashboards/superset",
        "dbt/models/bronze",
        "dbt/models/silver",
        "dbt/models/gold",
        "deploy/helm",
        "infra/terraform",
        "ingestion/cvm",
        "ingestion/bcb",
        "ingestion/b3",
        "metadata/openmetadata",
        "monitoring/prometheus",
        "monitoring/grafana",
        "monitoring/loki",
        "orchestration/dags",
        "quality/contracts",
        "quality/expectations",
        "scripts",
        "src/apex_lakehouse",
        "tests/unit",
        "tests/integration",
        "tests/e2e",
    ]

    missing = [path for path in expected_dirs if not (ROOT / path).is_dir()]
    assert not missing, f"Missing scaffold directories: {missing}"


def test_gitignore_protects_local_workspace() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert ".local/" in gitignore
    assert ".env" in gitignore
