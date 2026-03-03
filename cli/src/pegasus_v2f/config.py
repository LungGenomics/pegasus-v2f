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


# ---------------------------------------------------------------------------
# Study helpers (read from in-memory config dict)
# ---------------------------------------------------------------------------

# Reserved id_prefix values that collide with CLI subcommands
_RESERVED_STUDY_IDS = {"list", "add"}


def get_study_list(config: dict) -> list[dict]:
    """Extract the pegasus.study list from config. Always returns a list."""
    study = config.get("pegasus", {}).get("study")
    if study is None:
        return []
    if isinstance(study, dict):
        # Legacy single-dict format → wrap in list
        return [study]
    return list(study)


def get_study_by_id(config: dict, id_prefix: str) -> dict | None:
    """Look up a single study by id_prefix. Returns None if not found."""
    for s in get_study_list(config):
        if s.get("id_prefix") == id_prefix:
            return s
    return None


# ---------------------------------------------------------------------------
# Study CRUD (modify v2f.yaml in place)
# ---------------------------------------------------------------------------

def add_study_to_yaml(
    config_path: Path,
    study_config: dict,
    locus_config: dict | None = None,
) -> None:
    """Append a study to pegasus.study list in v2f.yaml.

    Creates the pegasus.study list if it doesn't exist.
    Raises ValueError on duplicate id_prefix or reserved name.
    """
    prefix = study_config.get("id_prefix", "")
    if prefix in _RESERVED_STUDY_IDS:
        raise ValueError(
            f"Study id_prefix '{prefix}' is reserved (conflicts with CLI subcommand). "
            f"Choose a different name."
        )

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    config.setdefault("pegasus", {})
    existing = config["pegasus"].get("study")

    # Normalise to list
    if existing is None:
        studies = []
    elif isinstance(existing, dict):
        studies = [existing]
    else:
        studies = list(existing)

    # Reject duplicates
    if any(s.get("id_prefix") == prefix for s in studies):
        raise ValueError(f"Study '{prefix}' already exists")

    studies.append(study_config)
    config["pegasus"]["study"] = studies

    if locus_config and "locus_definition" not in config["pegasus"]:
        config["pegasus"]["locus_definition"] = locus_config

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def remove_study_from_yaml(config_path: Path, id_prefix: str) -> None:
    """Remove a study from pegasus.study list. Raises ValueError if not found."""
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    studies = get_study_list(config)
    new_studies = [s for s in studies if s.get("id_prefix") != id_prefix]
    if len(new_studies) == len(studies):
        raise ValueError(f"Study '{id_prefix}' not found")

    config.setdefault("pegasus", {})
    config["pegasus"]["study"] = new_studies

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def update_study_in_yaml(
    config_path: Path, id_prefix: str, key: str, value: Any
) -> None:
    """Update a scalar field on a study. Raises ValueError on bad key or missing study."""
    allowed_keys = {"gwas_source", "ancestry", "genome_build"}
    if key == "id_prefix":
        raise ValueError("id_prefix is immutable — cannot be changed after creation")
    if key not in allowed_keys:
        raise ValueError(
            f"Key '{key}' is not settable. Allowed: {', '.join(sorted(allowed_keys))}"
        )

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    studies = get_study_list(config)
    found = False
    for s in studies:
        if s.get("id_prefix") == id_prefix:
            s[key] = value
            found = True
            break

    if not found:
        raise ValueError(f"Study '{id_prefix}' not found")

    config.setdefault("pegasus", {})
    config["pegasus"]["study"] = studies

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def add_trait_to_study(config_path: Path, id_prefix: str, trait: str) -> None:
    """Append a trait to a study's traits list. Raises ValueError if not found or duplicate."""
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    studies = get_study_list(config)
    for s in studies:
        if s.get("id_prefix") == id_prefix:
            traits = s.get("traits", [])
            if trait in traits:
                raise ValueError(f"Trait '{trait}' already exists in study '{id_prefix}'")
            traits.append(trait)
            s["traits"] = traits
            break
    else:
        raise ValueError(f"Study '{id_prefix}' not found")

    config.setdefault("pegasus", {})
    config["pegasus"]["study"] = studies

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def remove_trait_from_study(config_path: Path, id_prefix: str, trait: str) -> None:
    """Remove a trait from a study. Raises ValueError if not found."""
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    studies = get_study_list(config)
    for s in studies:
        if s.get("id_prefix") == id_prefix:
            traits = s.get("traits", [])
            if trait not in traits:
                raise ValueError(f"Trait '{trait}' not found in study '{id_prefix}'")
            traits.remove(trait)
            s["traits"] = traits
            break
    else:
        raise ValueError(f"Study '{id_prefix}' not found")

    config.setdefault("pegasus", {})
    config["pegasus"]["study"] = studies

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def remove_source_from_yaml(config_path: Path, name: str) -> None:
    """Remove a data source entry from v2f.yaml by name."""
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    sources = config.get("data_sources", [])
    config["data_sources"] = [s for s in sources if s.get("name") != name]

    # Clean up orphaned evidence block under pegasus: (from earlier bug)
    if "pegasus" in config and "evidence" in config["pegasus"]:
        del config["pegasus"]["evidence"]

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def append_source_to_yaml(config_path: Path, source: dict) -> None:
    """Append a data source entry to v2f.yaml's data_sources list.

    Uses YAML round-trip to preserve existing formatting.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}

    config.setdefault("data_sources", [])

    # Don't duplicate
    if any(s.get("name") == source["name"] for s in config["data_sources"]):
        return

    config["data_sources"].append(source)

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


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
