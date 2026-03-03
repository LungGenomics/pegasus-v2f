"""Tests for sources.py — source CRUD operations."""

import pandas as pd
import pytest

from pegasus_v2f.db_schema import create_schema
from pegasus_v2f.sources import add_source, update_source, remove_source, list_sources


@pytest.fixture
def db_with_schema(conn):
    """DuckDB connection with core schema created."""
    create_schema(conn)
    return conn


def _make_csv(tmp_path, name="test.csv", content="gene,val\nSFTPC,1\nAGER,2\n"):
    """Write a CSV file and return its path."""
    p = tmp_path / name
    p.write_text(content)
    return p


def test_add_source(db_with_schema, tmp_path):
    """add_source loads data and registers in meta."""
    csv_path = _make_csv(tmp_path)
    source = {"name": "test_src", "source_type": "file", "path": str(csv_path)}
    rows = add_source(db_with_schema, source)
    assert rows == 2


def test_add_source_appears_in_list(db_with_schema, tmp_path):
    """Added source shows up in list_sources."""
    csv_path = _make_csv(tmp_path)
    source = {"name": "my_src", "source_type": "file", "path": str(csv_path)}
    add_source(db_with_schema, source)
    sources = list_sources(db_with_schema)
    assert len(sources) == 1
    assert sources[0]["name"] == "my_src"


def test_add_source_duplicate_rejected(db_with_schema, tmp_path):
    """Adding the same source name twice raises ValueError."""
    csv_path = _make_csv(tmp_path)
    source = {"name": "dup", "source_type": "file", "path": str(csv_path)}
    add_source(db_with_schema, source)
    with pytest.raises(ValueError, match="already exists"):
        add_source(db_with_schema, source)


def test_remove_source(db_with_schema, tmp_path):
    """remove_source drops table and removes from config."""
    csv_path = _make_csv(tmp_path)
    source = {"name": "to_remove", "source_type": "file", "path": str(csv_path)}
    add_source(db_with_schema, source)
    remove_source(db_with_schema, "to_remove")
    assert list_sources(db_with_schema) == []


def test_update_source(db_with_schema, tmp_path):
    """update_source re-loads data from stored config."""
    csv_path = _make_csv(tmp_path, content="gene,val\nA,1\n")
    source = {"name": "updatable", "source_type": "file", "path": str(csv_path)}
    add_source(db_with_schema, source)

    # Modify the file
    csv_path.write_text("gene,val\nA,1\nB,2\nC,3\n")
    rows = update_source(db_with_schema, "updatable")
    assert rows == 3


def test_update_nonexistent_source(db_with_schema):
    """update_source on unknown name raises ValueError."""
    with pytest.raises(ValueError, match="not found"):
        update_source(db_with_schema, "ghost")


def test_list_sources_empty(db_with_schema):
    """list_sources returns [] when no sources added."""
    assert list_sources(db_with_schema) == []
