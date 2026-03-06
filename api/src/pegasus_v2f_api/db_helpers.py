"""Shared database helpers for API route handlers."""

from __future__ import annotations

import math
from typing import Any

import yaml

from pegasus_v2f.db import is_postgres


def execute_query(
    conn: Any,
    sql: str,
    params: tuple | list | None = None,
    sql_postgres: str | None = None,
) -> tuple[list[str], list[tuple]]:
    """Execute a query with backend-appropriate syntax.

    Args:
        conn: Database connection (DuckDB or PostgreSQL).
        sql: SQL query with ``?`` placeholders (DuckDB style).
        params: Query parameters.
        sql_postgres: Override SQL for PostgreSQL (with ``%s`` placeholders).
            If not provided, ``?`` in *sql* is auto-replaced with ``%s``.

    Returns:
        (columns, rows) tuple.
    """
    if is_postgres(conn):
        pg_sql = sql_postgres or sql.replace("?", "%s")
        pg_params = tuple(params) if params else None
        cur = conn.cursor()
        try:
            cur.execute(pg_sql, pg_params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        finally:
            cur.close()
    else:
        result = conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    return columns, rows


def clean_value(val: Any) -> Any:
    """Replace None/NaN with ``'-'`` to match old Plumber API behavior."""
    if val is None:
        return "-"
    if isinstance(val, float) and math.isnan(val):
        return "-"
    return val


def clean_rows(columns: list[str], rows: list[tuple]) -> list[dict]:
    """Convert raw DB rows to list of dicts with None/NaN cleaned."""
    return [
        {col: clean_value(val) for col, val in zip(columns, row)}
        for row in rows
    ]


def has_table(conn: Any, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table_name,),
            )
            exists = cur.fetchone() is not None
            cur.close()
            return exists
        else:
            result = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result is not None
    except Exception:
        return False


def get_stored_config(conn: Any) -> dict:
    """Load the stored config from _pegasus_meta.

    Returns an empty dict if no config is stored.
    """
    from pegasus_v2f.db_meta import read_meta

    config_yaml = read_meta(conn, "config")
    if not config_yaml:
        return {}
    return yaml.safe_load(config_yaml) or {}
