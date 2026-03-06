"""Git-based config and data sync for project repos."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def sync_status(project_root: Path) -> dict:
    """Check git sync status for a project repo.

    Returns:
        Dict with keys: is_git, branch, ahead, behind, dirty, remote_url.
    """
    project_root = Path(project_root)

    if not (project_root / ".git").exists():
        return {"is_git": False}

    try:
        branch = _git(project_root, "rev-parse", "--abbrev-ref", "HEAD")
    except RuntimeError:
        # No commits yet (fresh git init)
        branch = None

    result = {
        "is_git": True,
        "branch": branch,
        "ahead": 0,
        "behind": 0,
        "dirty": False,
        "dirty_files": [],
        "remote_url": None,
    }

    # Check for uncommitted changes
    status_out = _git(project_root, "status", "--porcelain")
    if status_out:
        result["dirty"] = True
        result["dirty_files"] = [
            line[3:] for line in status_out.splitlines() if line.strip()
        ]

    # Get remote URL
    try:
        result["remote_url"] = _git(project_root, "remote", "get-url", "origin")
    except RuntimeError:
        pass

    # Fetch to get latest remote state
    try:
        _git(project_root, "fetch", "--quiet")
    except RuntimeError as e:
        logger.warning(f"git fetch failed: {e}")
        return result

    # Count ahead/behind
    try:
        counts = _git(project_root, "rev-list", "--left-right", "--count", "HEAD...@{u}")
        if counts:
            parts = counts.split()
            result["ahead"] = int(parts[0])
            result["behind"] = int(parts[1])
    except RuntimeError:
        # No upstream tracking branch
        pass

    return result


def sync_pull(project_root: Path) -> dict:
    """Pull remote changes.

    Returns:
        Dict with keys: pulled, commits_pulled, files_changed, config_changed.
    """
    project_root = Path(project_root)
    status = sync_status(project_root)

    if not status["is_git"]:
        raise RuntimeError("Not a git repository — nothing to sync")

    if status["dirty"]:
        raise RuntimeError(
            f"Working directory has uncommitted changes: {', '.join(status['dirty_files'])}. "
            "Commit or stash them before syncing."
        )

    if status["behind"] == 0:
        return {"pulled": False, "commits_pulled": 0, "files_changed": [], "config_changed": False}

    # Get list of files that will change
    diff_out = _git(project_root, "diff", "--name-only", "HEAD...@{u}")
    files_changed = [f for f in diff_out.splitlines() if f.strip()]

    # Pull
    _git(project_root, "pull", "--ff-only")

    config_changed = "v2f.yaml" in files_changed

    return {
        "pulled": True,
        "commits_pulled": status["behind"],
        "files_changed": files_changed,
        "config_changed": config_changed,
    }


def sync_push(project_root: Path, message: str | None = None) -> dict:
    """Stage config files and push to remote.

    Stages v2f.yaml and any data files, commits, and pushes.

    Returns:
        Dict with keys: pushed, files_staged.
    """
    project_root = Path(project_root)
    status = sync_status(project_root)

    if not status["is_git"]:
        raise RuntimeError("Not a git repository — nothing to push")

    if not status["remote_url"]:
        raise RuntimeError("No remote configured — cannot push")

    # Stage trackable project files
    files_to_stage = []
    for pattern in ["v2f.yaml", "data/"]:
        target = project_root / pattern
        if target.exists():
            files_to_stage.append(pattern)

    if not files_to_stage:
        return {"pushed": False, "files_staged": []}

    for f in files_to_stage:
        _git(project_root, "add", f)

    # Check if there's anything staged
    staged = _git(project_root, "diff", "--cached", "--name-only")
    if not staged:
        return {"pushed": False, "files_staged": []}

    staged_files = [f for f in staged.splitlines() if f.strip()]

    # Commit and push
    commit_msg = message or "v2f: sync config and data"
    _git(project_root, "commit", "-m", commit_msg)
    _git(project_root, "push")

    return {"pushed": True, "files_staged": staged_files}


def _git(cwd: Path, *args: str) -> str:
    """Run a git command and return stdout. Raises RuntimeError on failure."""
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {args[0]} failed")
    return result.stdout.strip()
