"""Source management CRUD — add, update, remove, list data sources."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pegasus_v2f.db import is_postgres, write_table
from pegasus_v2f.db_meta import read_meta, write_meta
from pegasus_v2f.loaders import load_source
from pegasus_v2f.transform import apply_transformations, clean_for_db

logger = logging.getLogger(__name__)


def add_source(
    conn: Any,
    source: dict,
    data_dir: Path | None = None,
    config: dict | None = None,
    no_score: bool = False,
) -> int:
    """Add a new data source: load data, write table, update stored config.

    If the source has an ``evidence`` block, routes through evidence loaders
    instead of writing a raw table.  Auto-runs scoring unless *no_score* is set.

    Args:
        conn: Open database connection.
        source: Source config dict (name, source_type, url/path, etc.).
        data_dir: Base directory for resolving relative file paths.
        config: Full resolved config (needed for evidence routing / scoring).
        no_score: Skip auto-scoring after loading (for batch operations).

    Returns:
        Number of rows loaded.
    """
    name = source["name"]

    # Check if source already exists
    existing = list_sources(conn)
    if any(s["name"] == name for s in existing):
        raise ValueError(f"Source '{name}' already exists. Use --force to replace it.")

    # Load and transform
    df = load_source(source, data_dir=data_dir)
    transformations = source.get("transformations", [])
    if transformations:
        df = apply_transformations(df, transformations)
    df = clean_for_db(df)

    evidence = source.get("evidence")
    if evidence and config:
        # Evidence-aware path
        from pegasus_v2f.evidence import route_evidence_source
        route_evidence_source(conn, source, df, config)

        if not no_score and config.get("pegasus"):
            from pegasus_v2f.scoring import compute_locus_gene_scores
            compute_locus_gene_scores(conn, config)
    else:
        # Raw table path
        write_table(conn, name, df)

    # Update stored config
    _append_source_to_meta(conn, source)

    # Update source_metadata table (only for raw sources)
    if not evidence:
        try:
            _upsert_source_metadata(conn, source, len(df))
        except Exception:
            pass  # source_metadata may not exist outside full builds

    logger.info(f"Added source '{name}': {len(df)} rows")
    return len(df)


def update_source(
    conn: Any,
    name: str,
    data_dir: Path | None = None,
    config: dict | None = None,
) -> int:
    """Re-fetch and reload an existing source from its stored config.

    For evidence sources: deletes old evidence rows by source_tag, re-loads
    through evidence routing, and re-runs scoring.

    Returns:
        Number of rows loaded.
    """
    sources = list_sources(conn)
    source = next((s for s in sources if s["name"] == name), None)
    if not source:
        raise ValueError(f"Source '{name}' not found in stored config")

    # Load and transform
    df = load_source(source, data_dir=data_dir)
    transformations = source.get("transformations", [])
    if transformations:
        df = apply_transformations(df, transformations)
    df = clean_for_db(df)

    evidence = source.get("evidence")
    if evidence and config:
        # Delete old evidence rows by source_tag
        source_tag = evidence.get("source_tag", "")
        if source_tag:
            _delete_evidence_by_source_tag(conn, source_tag)

        # Re-load through evidence routing
        from pegasus_v2f.evidence import route_evidence_source
        route_evidence_source(conn, source, df, config)

        # Re-score
        if config.get("pegasus"):
            from pegasus_v2f.scoring import compute_locus_gene_scores
            compute_locus_gene_scores(conn, config)
    else:
        # Replace raw table
        write_table(conn, name, df)
        try:
            _upsert_source_metadata(conn, source, len(df))
        except Exception:
            pass  # source_metadata may not exist outside full builds

    logger.info(f"Updated source '{name}': {len(df)} rows")
    return len(df)


def remove_source(conn: Any, name: str, config: dict | None = None) -> None:
    """Drop a source table and remove it from stored config.

    For evidence sources: deletes evidence rows by source_tag, removes from
    data_sources table, and re-runs scoring.
    """
    # Look up the source to check for evidence block
    sources = list_sources(conn)
    source = next((s for s in sources if s["name"] == name), None)

    evidence = source.get("evidence") if source else None
    if evidence:
        source_tag = evidence.get("source_tag", "")
        role = evidence.get("role")

        if source_tag:
            _delete_evidence_by_source_tag(conn, source_tag)

            # Locus definition sources also create studies, loci, and scores
            if role == "locus_definition":
                _delete_locus_definition_data(conn, source_tag)

            # Remove from data_sources provenance table
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute("DELETE FROM data_sources WHERE source_tag = %s", (source_tag,))
                conn.commit()
                cur.close()
            else:
                conn.execute("DELETE FROM data_sources WHERE source_tag = ?", [source_tag])

        # Re-score
        if config and config.get("pegasus"):
            from pegasus_v2f.scoring import compute_locus_gene_scores
            compute_locus_gene_scores(conn, config)
    else:
        # Raw table — drop it
        conn.execute(f'DROP TABLE IF EXISTS "{name}"')

        # Remove from source_metadata
        try:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute("DELETE FROM source_metadata WHERE table_name = %s", (name,))
                conn.commit()
                cur.close()
            else:
                conn.execute("DELETE FROM source_metadata WHERE table_name = ?", (name,))
        except Exception:
            pass  # source_metadata may not exist in PEGASUS builds

    # Remove from stored config
    _remove_source_from_meta(conn, name)

    logger.info(f"Removed source '{name}'")


def list_sources(conn: Any) -> list[dict]:
    """List all data sources from stored config in _pegasus_meta."""
    config_yaml = read_meta(conn, "config")
    if not config_yaml:
        return []

    config = yaml.safe_load(config_yaml)
    return config.get("data_sources", [])


def _delete_locus_definition_data(conn: Any, source_tag: str) -> None:
    """Delete orphaned loci, scores, and studies after evidence rows were removed.

    Called after _delete_evidence_by_source_tag has already removed the
    locus_gene_evidence rows.  This cleans up loci that no longer have any
    evidence, their scores, and studies with no remaining loci.
    """
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM locus_gene_scores WHERE locus_id NOT IN "
                "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
            )
            cur.execute(
                "DELETE FROM loci WHERE locus_id NOT IN "
                "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
            )
            cur.execute(
                "DELETE FROM studies WHERE study_id NOT IN "
                "(SELECT DISTINCT study_id FROM loci)"
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                "DELETE FROM locus_gene_scores WHERE locus_id NOT IN "
                "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
            )
            conn.execute(
                "DELETE FROM loci WHERE locus_id NOT IN "
                "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
            )
            conn.execute(
                "DELETE FROM studies WHERE study_id NOT IN "
                "(SELECT DISTINCT study_id FROM loci)"
            )
    except Exception:
        pass  # Tables may not exist


def _delete_evidence_by_source_tag(conn: Any, source_tag: str) -> None:
    """Delete evidence rows from both evidence tables for a given source_tag."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM locus_gene_evidence WHERE source_tag = %s", (source_tag,)
        )
        cur.execute(
            "DELETE FROM gene_evidence WHERE source_tag = %s", (source_tag,)
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "DELETE FROM locus_gene_evidence WHERE source_tag = ?", [source_tag]
        )
        conn.execute(
            "DELETE FROM gene_evidence WHERE source_tag = ?", [source_tag]
        )


def _append_source_to_meta(conn: Any, source: dict) -> None:
    """Add a source entry to the stored config in _pegasus_meta."""
    config_yaml = read_meta(conn, "config")
    if config_yaml:
        config = yaml.safe_load(config_yaml)
    else:
        config = {"version": 1, "data_sources": []}

    config.setdefault("data_sources", [])
    config["data_sources"].append(source)

    write_meta(conn, "config", yaml.dump(config, default_flow_style=False, sort_keys=False))


def update_source_in_meta(conn: Any, name: str, updates: dict) -> None:
    """Merge fields into a source entry in _pegasus_meta by name.

    Used by integrate to sync the evidence block back to the embedded config.
    """
    config_yaml = read_meta(conn, "config")
    if not config_yaml:
        return

    config = yaml.safe_load(config_yaml)
    for source in config.get("data_sources", []):
        if source.get("name") == name:
            source.update(updates)
            break

    write_meta(conn, "config", yaml.dump(config, default_flow_style=False, sort_keys=False))


def _remove_source_from_meta(conn: Any, name: str) -> None:
    """Remove a source entry from the stored config in _pegasus_meta."""
    config_yaml = read_meta(conn, "config")
    if not config_yaml:
        return

    config = yaml.safe_load(config_yaml)
    sources = config.get("data_sources", [])
    config["data_sources"] = [s for s in sources if s.get("name") != name]

    write_meta(conn, "config", yaml.dump(config, default_flow_style=False, sort_keys=False))


def _upsert_source_metadata(conn: Any, source: dict, row_count: int) -> None:
    """Insert or update a row in the source_metadata table."""
    now = datetime.now(timezone.utc).isoformat()

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO source_metadata (table_name, display_name, description, data_type,
                source_type, gene_column, unique_per_gene, include_in_search, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                last_updated = EXCLUDED.last_updated
            """,
            (
                source["name"],
                source.get("display_name", source["name"]),
                source.get("description", ""),
                source.get("data_type", ""),
                source.get("source_type", ""),
                source.get("gene_column", "gene"),
                source.get("unique_per_gene", True),
                source.get("include_in_search", True),
                now,
            ),
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            """
            INSERT OR REPLACE INTO source_metadata
                (table_name, display_name, description, data_type,
                 source_type, gene_column, unique_per_gene, include_in_search, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source["name"],
                source.get("display_name", source["name"]),
                source.get("description", ""),
                source.get("data_type", ""),
                source.get("source_type", ""),
                source.get("gene_column", "gene"),
                source.get("unique_per_gene", True),
                source.get("include_in_search", True),
                now,
            ),
        )


