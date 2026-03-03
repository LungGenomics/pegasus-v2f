"""Tests for db_meta.py — _pegasus_meta table lifecycle."""

import pytest

from pegasus_v2f.db_meta import (
    ensure_meta_table,
    write_meta,
    read_meta,
    read_all_meta,
    write_build_meta,
)


def test_ensure_meta_table(conn):
    """Creates _pegasus_meta table."""
    ensure_meta_table(conn)
    result = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = '_pegasus_meta'"
    ).fetchone()
    assert result is not None


def test_ensure_meta_table_idempotent(conn):
    """Calling ensure_meta_table twice doesn't error."""
    ensure_meta_table(conn)
    ensure_meta_table(conn)


def test_write_and_read_meta(conn):
    """Write a key-value pair and read it back."""
    write_meta(conn, "test_key", "test_value")
    assert read_meta(conn, "test_key") == "test_value"


def test_read_meta_missing_key(conn):
    """Reading a non-existent key returns None."""
    ensure_meta_table(conn)
    assert read_meta(conn, "nonexistent") is None


def test_write_meta_upsert(conn):
    """Writing the same key twice updates the value."""
    write_meta(conn, "k", "v1")
    write_meta(conn, "k", "v2")
    assert read_meta(conn, "k") == "v2"


def test_read_all_meta(conn):
    """read_all_meta returns all key-value pairs."""
    write_meta(conn, "a", "1")
    write_meta(conn, "b", "2")
    result = read_all_meta(conn)
    assert result["a"] == "1"
    assert result["b"] == "2"


def test_write_build_meta(conn):
    """write_build_meta stores config, version, timestamp, genome_build."""
    write_build_meta(conn, "version: 1\ndata_sources: []\n", genome_build="hg38")
    assert read_meta(conn, "config") == "version: 1\ndata_sources: []\n"
    assert read_meta(conn, "genome_build") == "hg38"
    assert read_meta(conn, "package_version") is not None
    assert read_meta(conn, "build_timestamp") is not None
