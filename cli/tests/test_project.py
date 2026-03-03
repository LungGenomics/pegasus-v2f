"""Tests for project.py — project discovery and initialization."""

import pytest
from pathlib import Path

from pegasus_v2f.project import find_project_root, init_project


def test_find_project_root(tmp_project):
    """Finds project root from the project dir itself."""
    assert find_project_root(tmp_project) == tmp_project


def test_find_project_root_from_subdir(tmp_project):
    """Finds project root from a subdirectory."""
    subdir = tmp_project / "data" / "raw"
    subdir.mkdir(parents=True)
    assert find_project_root(subdir) == tmp_project


def test_find_project_root_not_found(tmp_path):
    """Returns None when no v2f.yaml exists."""
    assert find_project_root(tmp_path) is None


def test_init_project_creates_structure(tmp_path):
    """init_project creates v2f.yaml, .v2f/, .gitignore."""
    dest = tmp_path / "new_project"
    root = init_project(dest)
    assert (root / "v2f.yaml").exists()
    assert (root / ".v2f").is_dir()
    assert (root / ".gitignore").exists()


def test_init_project_rejects_existing_project(tmp_path):
    """init_project raises FileExistsError if v2f.yaml already exists."""
    dest = tmp_path / "existing"
    dest.mkdir()
    existing_config = dest / "v2f.yaml"
    existing_config.write_text("version: 99\n")
    with pytest.raises(FileExistsError, match="Already a v2f project"):
        init_project(dest)
