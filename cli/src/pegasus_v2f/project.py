"""Project discovery, initialization, and status."""

from __future__ import annotations

import shutil
import subprocess
from importlib import resources
from pathlib import Path

from pegasus_v2f.config import read_config, get_data_sources, get_database_config


def find_project_root(start: Path | str | None = None) -> Path | None:
    """Walk up from start looking for v2f.yaml. Returns None if not found."""
    current = Path(start) if start else Path.cwd()
    current = current.resolve()

    while True:
        if (current / "v2f.yaml").exists():
            return current
        parent = current.parent
        if parent == current:
            return None
        current = parent


def init_project(dest: Path | str, url: str | None = None) -> Path:
    """Initialize a new v2f project directory.

    If url is provided, clones the repo first. Otherwise creates a fresh project.
    Returns the project root path.
    """
    dest = Path(dest).resolve()

    if url:
        # Clone the repo
        subprocess.run(["git", "clone", url, str(dest)], check=True)
    else:
        dest.mkdir(parents=True, exist_ok=True)

    # Create .v2f/ directory
    v2f_dir = dest / ".v2f"
    v2f_dir.mkdir(exist_ok=True)

    # Write template files if they don't exist
    config_path = dest / "v2f.yaml"
    if not config_path.exists():
        template = resources.files("pegasus_v2f") / "templates" / "v2f.yaml"
        config_path.write_text(template.read_text())

    gitignore_path = dest / ".gitignore"
    if not gitignore_path.exists():
        template = resources.files("pegasus_v2f") / "templates" / "gitignore"
        gitignore_path.write_text(template.read_text())

    return dest


def project_status(project_root: Path | str) -> dict:
    """Gather project status information."""
    project_root = Path(project_root)
    config = read_config(project_root)
    db_config = get_database_config(config)
    sources = get_data_sources(config)

    # Check database
    db_path = project_root / ".v2f" / db_config.get("name", "gene.duckdb")
    db_exists = db_path.exists()
    db_size = db_path.stat().st_size if db_exists else 0

    # Check git status
    git_dir = project_root / ".git"
    git_status = None
    if git_dir.exists():
        try:
            result = subprocess.run(
                ["git", "log", "--oneline", "-1", "HEAD..@{u}"],
                capture_output=True, text=True, cwd=project_root,
            )
            behind = len(result.stdout.strip().splitlines()) if result.stdout.strip() else 0
            git_status = {"behind": behind}
        except Exception:
            git_status = None

    # Check local overrides
    local_path = project_root / ".v2f" / "local.yaml"

    return {
        "project_root": str(project_root),
        "config_file": str(project_root / "v2f.yaml"),
        "sources_count": len(sources),
        "has_local_overrides": local_path.exists(),
        "database": {
            "backend": db_config.get("backend", "duckdb"),
            "path": str(db_path) if db_config.get("backend", "duckdb") == "duckdb" else None,
            "exists": db_exists,
            "size_bytes": db_size,
        },
        "git": git_status,
    }
