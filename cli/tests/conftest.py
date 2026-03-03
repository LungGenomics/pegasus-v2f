"""Shared fixtures for v2f tests."""

import pytest
import duckdb


@pytest.fixture
def conn():
    """In-memory DuckDB connection, closed after test."""
    c = duckdb.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def tmp_project(tmp_path):
    """Minimal v2f project directory with config and .v2f/."""
    config = tmp_path / "v2f.yaml"
    config.write_text(
        "version: 1\n"
        "database:\n"
        "  backend: duckdb\n"
        "  name: gene.duckdb\n"
        "data_sources: []\n"
    )
    v2f_dir = tmp_path / ".v2f"
    v2f_dir.mkdir()
    return tmp_path
