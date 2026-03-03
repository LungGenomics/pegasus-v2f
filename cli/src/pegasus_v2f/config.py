"""Config loading, merging, and resolution."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


def read_config(project_root: Path | str) -> dict:
    """Read and merge config from a project directory.

    Loads v2f.yaml, then deep-merges .v2f/local.yaml on top.
    """
    project_root = Path(project_root)
    base_path = project_root / "v2f.yaml"

    if not base_path.exists():
        raise FileNotFoundError(f"No v2f.yaml found in {project_root}")

    with open(base_path) as f:
        config = yaml.safe_load(f) or {}

    # Merge local overrides
    local_path = project_root / ".v2f" / "local.yaml"
    if local_path.exists():
        with open(local_path) as f:
            local = yaml.safe_load(f) or {}
        config = merge_configs(config, local)

    # Env var overrides
    config = _apply_env_overrides(config)

    return config


def merge_configs(base: dict, override: dict) -> dict:
    """Deep-merge override into base. Override values win."""
    result = deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _apply_env_overrides(config: dict) -> dict:
    """Apply environment variable overrides to config."""
    # V2F_DATABASE_URL overrides the entire database backend
    url = os.environ.get("V2F_DATABASE_URL")
    if url:
        config.setdefault("database", {})
        if url.startswith("postgresql://") or url.startswith("postgres://"):
            config["database"]["backend"] = "postgres"
        # The actual URL is handled by db.py's get_connection()

    return config


def get_data_sources(config: dict) -> list[dict]:
    """Extract the data_sources list from config."""
    return config.get("data_sources", [])


def get_database_config(config: dict) -> dict:
    """Extract database section with defaults."""
    db = config.get("database", {})
    db.setdefault("backend", "duckdb")
    db.setdefault("genome_build", "hg38")
    db.setdefault("name", "gene.duckdb")
    return db


def config_to_yaml(config: dict) -> str:
    """Serialize config dict to YAML string."""
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def validate_config(config: dict) -> list[str]:
    """Validate config structure. Returns list of error messages (empty = valid)."""
    errors = []

    if "version" not in config:
        errors.append("Missing required field: version")

    db = config.get("database", {})
    backend = db.get("backend", "duckdb")
    if backend not in ("duckdb", "postgres"):
        errors.append(f"Invalid database.backend: {backend} (must be duckdb or postgres)")

    if backend == "postgres" and "postgres" not in db:
        errors.append("database.backend is 'postgres' but database.postgres section is missing")

    if backend == "postgres":
        pg = db.get("postgres", {})
        if "dbname" not in pg:
            errors.append("database.postgres.dbname is required")

    sources = config.get("data_sources", [])
    names = set()
    for i, src in enumerate(sources):
        if "name" not in src:
            errors.append(f"data_sources[{i}]: missing required field 'name'")
        else:
            if src["name"] in names:
                errors.append(f"data_sources[{i}]: duplicate name '{src['name']}'")
            names.add(src["name"])

        if "source_type" not in src:
            errors.append(f"data_sources[{i}]: missing required field 'source_type'")

    # PEGASUS evidence validation (when pegasus: section present)
    if config.get("pegasus"):
        from pegasus_v2f.evidence_config import (
            validate_evidence_config,
            validate_pegasus_config,
        )

        errors.extend(validate_pegasus_config(config))
        for src in sources:
            errors.extend(validate_evidence_config(src))

    return errors
