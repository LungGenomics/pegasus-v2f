"""Database schema — DDL for core tables and PEGASUS stubs."""

from __future__ import annotations

from typing import Any

from pegasus_v2f.db import is_postgres
from pegasus_v2f.db_meta import META_DDL
from pegasus_v2f.pegasus_schema import create_pegasus_schema


# -- Core tables (used now) --

SOURCE_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS source_metadata (
    table_name TEXT PRIMARY KEY,
    display_name TEXT,
    description TEXT,
    data_type TEXT,
    source_type TEXT,
    gene_column TEXT,
    unique_per_gene BOOLEAN,
    include_in_search BOOLEAN,
    last_updated TIMESTAMP
)
"""

GENE_ANNOTATIONS_DDL = """
CREATE TABLE IF NOT EXISTS gene_annotations (
    ensembl_gene_id TEXT PRIMARY KEY,
    gene TEXT,
    chromosome TEXT,
    start_position INTEGER,
    end_position INTEGER,
    strand INTEGER,
    genome_build TEXT
)
"""

GENE_SEARCH_INDEX_DDL = """
CREATE TABLE IF NOT EXISTS gene_search_index (
    ensembl_gene_id TEXT PRIMARY KEY,
    gene TEXT,
    chromosome TEXT,
    start_position INTEGER,
    end_position INTEGER,
    searchable_text TEXT
)
"""

GENE_TABLE_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS gene_table_summary (
    table_name TEXT,
    display_name TEXT,
    gene TEXT,
    row_count INTEGER
)
"""

GENE_TABLE_SUMMARY_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_gene_table_gene ON gene_table_summary (gene)",
    "CREATE INDEX IF NOT EXISTS idx_gene_table_table ON gene_table_summary (table_name)",
]


# -- All core DDL in order --

CORE_DDL = [
    META_DDL,
    SOURCE_METADATA_DDL,
    GENE_ANNOTATIONS_DDL,
    GENE_SEARCH_INDEX_DDL,
    GENE_TABLE_SUMMARY_DDL,
    *GENE_TABLE_SUMMARY_INDEXES,
]


def create_schema(conn: Any, config: dict | None = None) -> None:
    """Create all core tables if they don't exist.

    If config has a ``pegasus:`` section, also creates PEGASUS evidence
    model tables (genes, variants, studies, loci, evidence, scores, etc.).
    """
    for ddl in CORE_DDL:
        conn.execute(ddl)
    if is_postgres(conn):
        conn.commit()

    if config and config.get("pegasus"):
        create_pegasus_schema(conn)


def list_tables(conn: Any) -> list[dict]:
    """List all tables with row counts."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        tables = [row[0] for row in cur.fetchall()]
        result = []
        for t in tables:
            cur.execute(f'SELECT COUNT(*) FROM "{t}"')
            count = cur.fetchone()[0]
            result.append({"table": t, "rows": count})
        cur.close()
        return result
    else:
        tables = conn.execute("SHOW TABLES").fetchall()
        result = []
        for (t,) in tables:
            count = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            result.append({"table": t, "rows": count})
        return result


def has_tables(conn: Any) -> bool:
    """Check if the database has any user tables (non-empty DB)."""
    return len(list_tables(conn)) > 0


def drop_all_tables(conn: Any) -> None:
    """Drop all tables. Used by build --overwrite."""
    tables = list_tables(conn)
    for t in tables:
        conn.execute(f'DROP TABLE IF EXISTS "{t["table"]}"')
    if is_postgres(conn):
        conn.commit()
