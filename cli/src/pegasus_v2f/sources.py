"""Source management CRUD — add, update, remove, list data sources."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pegasus_v2f.db import is_postgres, raw_table_name, write_table
from pegasus_v2f.db_meta import read_meta, write_meta
from pegasus_v2f.loaders import load_source
from pegasus_v2f.report import Report
from pegasus_v2f.transform import apply_transformations, clean_for_db

logger = logging.getLogger(__name__)


def add_source(
    conn: Any,
    source: dict,
    data_dir: Path | None = None,
    config: dict | None = None,
    no_score: bool = False,
    report: Report | None = None,
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
    load_report = report.child("load") if report else None
    df = load_source(source, data_dir=data_dir, report=load_report)
    transformations = source.get("transformations", [])
    if transformations:
        transform_report = report.child("transform") if report else None
        df = apply_transformations(df, transformations, report=transform_report)
    df = clean_for_db(df)

    # Always write the raw table (all original columns, queryable)
    write_table(conn, raw_table_name(name), df)

    evidence_blocks = source.get("evidence") or []
    if evidence_blocks:
        # Load evidence into the unified evidence table
        from pegasus_v2f.evidence_loader import load_all_evidence
        evidence_report = report.child("evidence") if report else None
        load_all_evidence(conn, source, df, report=evidence_report)

    # Update stored config
    _append_source_to_meta(conn, source)

    # Update source_metadata table
    try:
        _upsert_source_metadata(conn, source, len(df))
    except Exception:
        pass  # source_metadata may not exist outside full builds

    if report:
        report.counters["rows_out"] = len(df)

    logger.info(f"Added source '{name}': {len(df)} rows")
    return len(df)


def update_source(
    conn: Any,
    name: str,
    data_dir: Path | None = None,
    config: dict | None = None,
    report: Report | None = None,
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
    load_report = report.child("load") if report else None
    df = load_source(source, data_dir=data_dir, report=load_report)
    transformations = source.get("transformations", [])
    if transformations:
        transform_report = report.child("transform") if report else None
        df = apply_transformations(df, transformations, report=transform_report)
    df = clean_for_db(df)

    # Always refresh the raw table
    write_table(conn, raw_table_name(name), df)

    evidence_blocks = source.get("evidence") or []
    if evidence_blocks:
        # Re-load evidence (loader handles cleanup per source_tag)
        from pegasus_v2f.evidence_loader import load_all_evidence
        evidence_report = report.child("evidence") if report else None
        load_all_evidence(conn, source, df, report=evidence_report)
    else:
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

    # Always drop the raw table
    raw_name = raw_table_name(name)
    conn.execute(f'DROP TABLE IF EXISTS "{raw_name}"')

    evidence_blocks = (source.get("evidence") or []) if source else []
    if evidence_blocks:
        for block in evidence_blocks:
            source_tag = block.get("source_tag", "")
            role = block.get("role")

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

        # Note: rescoring is now a separate step via `v2f rescore`
    else:
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
    """Delete orphaned scored_evidence, loci, and studies.

    Called after _delete_evidence_by_source_tag has already removed the
    evidence rows. Cleans up scored_evidence referencing removed evidence,
    then orphaned loci and studies.
    """
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM scored_evidence WHERE source_tag = %s", (source_tag,)
            )
            cur.execute(
                "DELETE FROM studies WHERE study_id NOT IN "
                "(SELECT DISTINCT study_id FROM loci)"
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                "DELETE FROM scored_evidence WHERE source_tag = ?", [source_tag]
            )
            conn.execute(
                "DELETE FROM studies WHERE study_id NOT IN "
                "(SELECT DISTINCT study_id FROM loci)"
            )
    except Exception:
        pass  # Tables may not exist


def _delete_evidence_by_source_tag(conn: Any, source_tag: str) -> None:
    """Delete evidence rows for a given source_tag."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM evidence WHERE source_tag = %s", (source_tag,)
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "DELETE FROM evidence WHERE source_tag = ?", [source_tag]
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


