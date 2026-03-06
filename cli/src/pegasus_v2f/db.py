"""Database connection management — DuckDB and PostgreSQL."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb


def raw_table_name(source_name: str) -> str:
    """Return the raw table name for a source (prefixed to distinguish from PEGASUS tables)."""
    return f"raw_{source_name}"


def get_connection(
    db: str | None = None,
    config: dict | None = None,
    read_only: bool = False,
    project_root: Path | str | None = None,
) -> Any:
    """Connect to DuckDB or PostgreSQL.

    Resolution order:
    1. Explicit db= argument (path or postgresql:// URL)
    2. V2F_DATABASE_URL env var
    3. Config dict (database.backend + settings)
    4. Default: .v2f/gene.duckdb in project_root (or current directory)
    """
    # Explicit db= wins
    if db:
        return _connect(db, read_only=read_only)

    # Env var
    url = os.environ.get("V2F_DATABASE_URL")
    if url:
        return _connect(url, read_only=read_only)

    base = Path(project_root) if project_root else Path.cwd()

    # Config-based
    if config:
        db_config = config.get("database", {})
        backend = db_config.get("backend", "duckdb")

        if backend == "postgres":
            return _connect_postgres(db_config["postgres"])

        # DuckDB from config
        name = db_config.get("name", "gene.duckdb")
        db_path = base / ".v2f" / name
        if read_only and not db_path.exists():
            raise FileNotFoundError(
                f"Database not found: {db_path}\n"
                "Run 'v2f source add' or 'v2f build' first to create the database."
            )
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(db_path), read_only=read_only)

    # Default fallback
    db_path = base / ".v2f" / "gene.duckdb"
    if read_only and not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run 'v2f source add' or 'v2f build' first to create the database."
        )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def _connect(db: str, read_only: bool = False) -> Any:
    """Route to the right backend based on the connection string."""
    if db.startswith("postgresql://") or db.startswith("postgres://"):
        return _connect_postgres_url(db)
    return duckdb.connect(db, read_only=read_only)


def _connect_postgres_url(url: str) -> Any:
    """Connect to PostgreSQL via connection URL."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "PostgreSQL support requires psycopg2. "
            "Install with: uv pip install 'pegasus-v2f[postgres]'"
        )
    return psycopg2.connect(url)


def _connect_postgres(pg_config: dict) -> Any:
    """Connect to PostgreSQL via config dict."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "PostgreSQL support requires psycopg2. "
            "Install with: uv pip install 'pegasus-v2f[postgres]'"
        )
    password = None
    password_env = pg_config.get("password_env")
    if password_env:
        password = os.environ.get(password_env)

    return psycopg2.connect(
        host=pg_config.get("host", "localhost"),
        port=pg_config.get("port", 5432),
        dbname=pg_config["dbname"],
        user=pg_config.get("user"),
        password=password,
    )


def is_postgres(conn: Any) -> bool:
    """Check if a connection is PostgreSQL (vs DuckDB)."""
    return type(conn).__module__.startswith("psycopg2")


def write_table(conn: Any, table_name: str, df) -> None:
    """Write a DataFrame to a database table (replace if exists).

    Works with both DuckDB and PostgreSQL connections.
    """
    if is_postgres(conn):
        from io import StringIO

        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        cols = []
        for col_name, dtype in df.dtypes.items():
            if "int" in str(dtype):
                sql_type = "INTEGER"
            elif "float" in str(dtype):
                sql_type = "DOUBLE PRECISION"
            elif "bool" in str(dtype):
                sql_type = "BOOLEAN"
            else:
                sql_type = "TEXT"
            cols.append(f'"{col_name}" {sql_type}')

        cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(cols)})')

        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, table_name, sep="\t", null="\\N")
        conn.commit()
        cur.close()
    else:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')


@contextmanager
def open_db(
    db: str | None = None,
    config: dict | None = None,
    read_only: bool = False,
    project_root: Path | str | None = None,
):
    """Context manager that opens and closes a database connection.

    Usage:
        with open_db(db="gene.duckdb") as conn:
            conn.execute("SELECT 1")
    """
    try:
        conn = get_connection(db=db, config=config, read_only=read_only, project_root=project_root)
    except FileNotFoundError as e:
        import sys
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    try:
        yield conn
    finally:
        conn.close()
