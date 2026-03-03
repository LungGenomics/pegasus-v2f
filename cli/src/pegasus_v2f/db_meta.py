"""Embedded config management via the _pegasus_meta table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pegasus_v2f import __version__
from pegasus_v2f.db import is_postgres


META_TABLE = "_pegasus_meta"

META_DDL = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_meta_table(conn: Any) -> None:
    """Create _pegasus_meta if it doesn't exist."""
    # Check if table already exists to avoid DDL on read-only connections
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (META_TABLE,),
        )
        exists = cur.fetchone() is not None
        cur.close()
    else:
        result = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [META_TABLE],
        ).fetchone()
        exists = result is not None

    if not exists:
        conn.execute(META_DDL)
        if is_postgres(conn):
            conn.commit()


def write_meta(conn: Any, key: str, value: str) -> None:
    """Write a key-value pair to _pegasus_meta (upsert)."""
    ensure_meta_table(conn)
    now = datetime.now(timezone.utc).isoformat()

    if is_postgres(conn):
        conn.execute(
            f"""
            INSERT INTO {META_TABLE} (key, value, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
            """,
            (key, value, now),
        )
        conn.commit()
    else:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {META_TABLE} (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            (key, value, now),
        )


def read_meta(conn: Any, key: str) -> str | None:
    """Read a value from _pegasus_meta. Returns None if not found."""
    ensure_meta_table(conn)

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f"SELECT value FROM {META_TABLE} WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
    else:
        row = conn.execute(
            f"SELECT value FROM {META_TABLE} WHERE key = ?", (key,)
        ).fetchone()

    return row[0] if row else None


def read_all_meta(conn: Any) -> dict[str, str]:
    """Read all key-value pairs from _pegasus_meta."""
    ensure_meta_table(conn)

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f"SELECT key, value FROM {META_TABLE}")
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(f"SELECT key, value FROM {META_TABLE}").fetchall()

    return {k: v for k, v in rows}


def write_build_meta(conn: Any, config_yaml: str, genome_build: str = "hg38") -> None:
    """Write standard build metadata after a successful build."""
    write_meta(conn, "config", config_yaml)
    write_meta(conn, "package_version", __version__)
    write_meta(conn, "build_timestamp", datetime.now(timezone.utc).isoformat())
    write_meta(conn, "genome_build", genome_build)
