"""Tests for db.py — connection management."""

import os
import pytest
import duckdb

from pegasus_v2f.db import get_connection, open_db, is_postgres


def test_get_connection_explicit_path(tmp_path):
    """Explicit db= path creates a DuckDB file."""
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db=db_path)
    conn.execute("SELECT 1")
    conn.close()
    assert (tmp_path / "test.duckdb").exists()


def test_get_connection_from_config(tmp_path):
    """Config-based DuckDB connection resolves to project_root/.v2f/name."""
    config = {"database": {"backend": "duckdb", "name": "mydb.duckdb"}}
    conn = get_connection(config=config, project_root=tmp_path)
    conn.execute("SELECT 1")
    conn.close()
    assert (tmp_path / ".v2f" / "mydb.duckdb").exists()


def test_get_connection_default_fallback(tmp_path):
    """No db, no config → default .v2f/gene.duckdb in project_root."""
    conn = get_connection(project_root=tmp_path)
    conn.execute("SELECT 1")
    conn.close()
    assert (tmp_path / ".v2f" / "gene.duckdb").exists()


def test_get_connection_env_var(tmp_path, monkeypatch):
    """V2F_DATABASE_URL env var overrides config."""
    db_path = str(tmp_path / "env.duckdb")
    monkeypatch.setenv("V2F_DATABASE_URL", db_path)
    conn = get_connection()
    conn.execute("SELECT 1")
    conn.close()
    assert (tmp_path / "env.duckdb").exists()


def test_open_db_context_manager(tmp_path):
    """open_db returns a connection that auto-closes."""
    db_path = str(tmp_path / "ctx.duckdb")
    with open_db(db=db_path) as conn:
        conn.execute("CREATE TABLE t (x INT)")
        conn.execute("INSERT INTO t VALUES (42)")
        result = conn.execute("SELECT x FROM t").fetchone()
        assert result[0] == 42


def test_is_postgres_false(conn):
    """DuckDB connection is not postgres."""
    assert is_postgres(conn) is False


def test_read_only_connection(tmp_path):
    """Read-only connection prevents writes."""
    db_path = str(tmp_path / "ro.duckdb")
    # Create DB first
    c = duckdb.connect(db_path)
    c.execute("CREATE TABLE t (x INT)")
    c.close()
    # Open read-only
    with open_db(db=db_path, read_only=True) as conn:
        with pytest.raises(Exception):
            conn.execute("INSERT INTO t VALUES (1)")
