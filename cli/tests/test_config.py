"""Tests for config.py — config resolution and validation."""

import pytest
import yaml

from pegasus_v2f.config import (
    read_config,
    merge_configs,
    validate_config,
    config_to_yaml,
    get_data_sources,
    get_database_config,
)


def test_read_config(tmp_project):
    """Reads v2f.yaml and returns a dict."""
    config = read_config(tmp_project)
    assert config["version"] == 1
    assert config["database"]["backend"] == "duckdb"


def test_read_config_with_local_override(tmp_project):
    """Local overrides are deep-merged."""
    local = tmp_project / ".v2f" / "local.yaml"
    local.write_text("database:\n  backend: postgres\n  postgres:\n    host: myhost\n    dbname: mydb\n")
    config = read_config(tmp_project)
    assert config["database"]["backend"] == "postgres"
    assert config["database"]["postgres"]["host"] == "myhost"
    # Original keys still present
    assert config["database"]["name"] == "gene.duckdb"


def test_merge_configs_deep():
    """Deep merge preserves nested keys from base."""
    base = {"a": {"x": 1, "y": 2}, "b": 3}
    override = {"a": {"y": 99, "z": 100}}
    result = merge_configs(base, override)
    assert result == {"a": {"x": 1, "y": 99, "z": 100}, "b": 3}


def test_merge_configs_override_scalar():
    """Override replaces scalar values."""
    result = merge_configs({"k": "old"}, {"k": "new"})
    assert result == {"k": "new"}


def test_validate_config_valid():
    """Valid config passes validation."""
    config = {
        "version": 1,
        "database": {"backend": "duckdb"},
        "data_sources": [{"name": "test", "source_type": "file"}],
    }
    errors = validate_config(config)
    assert errors == []


def test_validate_config_missing_version():
    """Missing version is an error."""
    errors = validate_config({"database": {}})
    assert any("version" in e.lower() for e in errors)


def test_validate_config_duplicate_names():
    """Duplicate source names are an error."""
    config = {
        "version": 1,
        "data_sources": [
            {"name": "dupe", "source_type": "file"},
            {"name": "dupe", "source_type": "file"},
        ],
    }
    errors = validate_config(config)
    assert any("duplicate" in e.lower() for e in errors)


def test_config_to_yaml():
    """config_to_yaml returns valid YAML string."""
    config = {"version": 1, "database": {"backend": "duckdb"}}
    output = config_to_yaml(config)
    parsed = yaml.safe_load(output)
    assert parsed["version"] == 1


def test_get_data_sources():
    """get_data_sources returns the data_sources list."""
    config = {"data_sources": [{"name": "a"}, {"name": "b"}]}
    assert len(get_data_sources(config)) == 2


def test_get_data_sources_empty():
    """get_data_sources returns [] when missing."""
    assert get_data_sources({}) == []


def test_get_database_config():
    """get_database_config returns the database section."""
    config = {"database": {"backend": "postgres"}}
    assert get_database_config(config)["backend"] == "postgres"
