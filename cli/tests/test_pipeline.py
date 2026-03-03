"""Tests for pipeline.py — build_db orchestration."""

import pytest
import duckdb

from pegasus_v2f.pipeline import build_db
from pegasus_v2f.db_schema import has_tables, create_schema


@pytest.fixture
def built_db(conn):
    """Build an empty database and return (conn, result)."""
    config = {
        "version": 1,
        "database": {"backend": "duckdb", "genome_build": "hg38"},
        "data_sources": [],
    }
    result = build_db(conn, config)
    return conn, result


def test_build_db_creates_schema(built_db):
    """build_db creates core tables."""
    conn, _ = built_db
    assert has_tables(conn)


def test_build_db_returns_summary(built_db):
    """build_db returns a summary dict with expected keys."""
    _, result = built_db
    assert result["sources_loaded"] == 0
    assert result["sources_total"] == 0
    assert result["genes_found"] == 0
    assert "tables" in result


def test_build_db_stores_meta(built_db):
    """build_db writes config to _pegasus_meta."""
    conn, _ = built_db
    from pegasus_v2f.db_meta import read_meta
    assert read_meta(conn, "config") is not None
    assert read_meta(conn, "genome_build") == "hg38"


def test_build_db_fails_on_nonempty(conn):
    """build_db refuses to build into a non-empty DB without --overwrite."""
    config = {
        "version": 1,
        "database": {"backend": "duckdb", "genome_build": "hg38"},
        "data_sources": [],
    }
    build_db(conn, config)
    with pytest.raises(RuntimeError, match="already has tables"):
        build_db(conn, config)


def test_build_db_overwrite(conn):
    """build_db with overwrite=True rebuilds."""
    config = {
        "version": 1,
        "database": {"backend": "duckdb", "genome_build": "hg38"},
        "data_sources": [],
    }
    build_db(conn, config)
    result = build_db(conn, config, overwrite=True)
    assert result["sources_loaded"] == 0
