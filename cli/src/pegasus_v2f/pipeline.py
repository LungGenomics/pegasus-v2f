"""Build pipeline — orchestrates loading, transforming, and writing data."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pegasus_v2f.config import (
    config_to_yaml,
    get_data_sources,
    get_database_config,
)
from pegasus_v2f.db import is_postgres, raw_table_name, write_table
from pegasus_v2f.db_meta import write_build_meta
from pegasus_v2f.db_schema import create_schema, has_tables, drop_all_tables
from pegasus_v2f.loaders import load_source
from pegasus_v2f.report import Report
from pegasus_v2f.transform import apply_transformations, clean_for_db
from pegasus_v2f.annotate import (
    create_gene_annotations,
    create_search_index,
    create_pegasus_search_index,
)

logger = logging.getLogger(__name__)


def build_db(
    conn: Any,
    config: dict,
    project_root: Path | None = None,
    overwrite: bool = False,
    report: Report | None = None,
) -> dict:
    """Build the entire database from config.

    Args:
        conn: Open database connection.
        config: Resolved config dict.
        project_root: Project root for resolving relative paths.
        overwrite: If True, drop all tables first. If False, fail on non-empty DB.

    Returns:
        Summary dict with counts and status.
    """
    # Check if DB already has tables
    if has_tables(conn):
        if overwrite:
            logger.info("Overwrite mode — dropping all existing tables")
            drop_all_tables(conn)
        else:
            raise RuntimeError("database_not_empty")

    # Create core schema (+ PEGASUS tables if config has pegasus: section)
    create_schema(conn, config=config)

    # Stash project_root in config for downstream consumers (e.g. cytoband cache)
    if project_root:
        config["_project_root"] = str(project_root)

    # Resolve data directory
    data_dir = None
    if project_root:
        data_dir = Path(project_root) / "data" / "raw"
        if not data_dir.exists():
            data_dir = Path(project_root)

    # Process data sources
    sources = get_data_sources(config)
    is_pegasus = bool(config.get("pegasus"))

    if is_pegasus:
        sources_report = report.child("sources") if report else None
        loaded_tables, all_genes, metadata_rows = process_data_sources_pegasus(
            conn, sources, data_dir, config, report=sources_report
        )
    else:
        loaded_tables, all_genes, metadata_rows = process_data_sources(
            conn, sources, data_dir
        )

    # Write source metadata (legacy — only for non-pegasus builds)
    if metadata_rows:
        meta_df = pd.DataFrame(metadata_rows)
        write_table(conn, "source_metadata", meta_df)

    # Gene annotations
    if all_genes:
        logger.info(f"Creating gene annotations for {len(all_genes)} genes")
        annotate_report = report.child("annotate") if report else None
        create_gene_annotations(conn, list(all_genes), config, report=annotate_report)
    else:
        logger.warning("No genes found — skipping annotations")
        if report:
            report.warning("no_genes", "no gene symbols found in any source — skipping annotations")

    if is_pegasus:
        # Scoring
        from pegasus_v2f.scoring import materialize_scored_evidence
        score_report = report.child("scoring") if report else None
        n_scored = materialize_scored_evidence(conn, config, report=score_report)
        logger.info(f"Scored {n_scored} locus-gene pairs")

        # PEGASUS search index (from evidence tables)
        create_pegasus_search_index(conn)
    else:
        # Legacy search index (from raw source tables)
        create_search_index(conn, sources, loaded_tables, config)

    # Write build metadata
    db_config = get_database_config(config)
    genome_build = db_config.get("genome_build", "hg38")
    write_build_meta(conn, config_to_yaml(config), genome_build=genome_build)

    return {
        "sources_loaded": len(loaded_tables),
        "sources_total": len(sources),
        "genes_found": len(all_genes),
        "tables": loaded_tables,
    }


def process_data_sources_pegasus(
    conn: Any,
    sources: list[dict],
    data_dir: Path | None,
    config: dict,
    report: Report | None = None,
) -> tuple[list[str], set[str], list[dict]]:
    """PEGASUS-aware multi-pass data source processing.

    Pass 1: Locus sources (locus_definition first, then gwas_sumstats)
    Pass 2: Other evidence sources (gene-centric, variant-centric)
    Pass 3: Raw sources (no evidence block → exploration tables)

    Returns:
        (loaded_table_names, all_gene_symbols, metadata_rows_for_raw_only)
    """
    from pegasus_v2f.evidence_loader import load_all_evidence

    loaded_tables = []
    all_genes: set[str] = set()
    metadata_rows = []  # only for raw (non-evidence) sources

    # Categorize sources
    locus_def_sources = []
    sumstats_sources = []
    other_evidence_sources = []
    raw_sources = []

    for source in sources:
        blocks = source.get("evidence") or []
        if not blocks:
            raw_sources.append(source)
        elif any(b.get("role") == "locus_definition" for b in blocks):
            locus_def_sources.append(source)
        elif any(b.get("role") == "gwas_sumstats" for b in blocks):
            sumstats_sources.append(source)
        else:
            other_evidence_sources.append(source)

    def _process_source(source: dict, pass_name: str) -> pd.DataFrame | None:
        """Load a source, report success/failure, return DataFrame or None."""
        name = source["name"]
        logger.info(f"{pass_name}: {name}")
        try:
            df = _load_and_transform(source, data_dir)
            write_table(conn, raw_table_name(name), df)
            result = load_all_evidence(conn, source, df)
            if result:
                logger.info(f"  {result}")
            loaded_tables.append(name)
            all_genes.update(_collect_genes(df))
            if report:
                report.info("source_loaded", f"{name}: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"  Failed: {e}")
            if report:
                report.error("source_failed", f"{name}: {e}")
            return None

    # Pass 1a: Locus definition sources
    for source in locus_def_sources:
        _process_source(source, "Pass 1a (locus_definition)")

    # Pass 1b: GWAS sumstats sources
    for source in sumstats_sources:
        _process_source(source, "Pass 1b (gwas_sumstats)")

    # Pass 2: Other evidence sources
    for source in other_evidence_sources:
        _process_source(source, "Pass 2 (evidence)")

    # Pass 3: Raw sources (exploration only)
    for source in raw_sources:
        name = source["name"]
        df = _process_source(source, "Pass 3 (raw)")
        if df is not None:
            metadata_rows.append({
                "table_name": raw_table_name(name),
                "display_name": source.get("display_name", name),
                "description": source.get("description", ""),
                "data_type": source.get("data_type", ""),
                "source_type": source.get("source_type", ""),
                "gene_column": "gene" if "gene" in df.columns else "",
                "unique_per_gene": source.get("unique_per_gene", True),
                "include_in_search": source.get("include_in_search", True),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            })

    if report:
        report.counters["sources_loaded"] = len(loaded_tables)
        report.counters["sources_total"] = len(sources)
        report.counters["sources_failed"] = len(sources) - len(loaded_tables)

    return loaded_tables, all_genes, metadata_rows


def process_data_sources(
    conn: Any,
    sources: list[dict],
    data_dir: Path | None,
) -> tuple[list[str], set[str], list[dict]]:
    """Legacy (non-PEGASUS) source processing — load each as a raw table.

    Returns:
        (loaded_table_names, all_gene_symbols, metadata_rows)
    """
    loaded_tables = []
    all_genes: set[str] = set()
    metadata_rows = []

    for source in sources:
        name = source["name"]
        logger.info(f"Processing source: {name}")

        try:
            df = _load_and_transform(source, data_dir)

            all_genes.update(_collect_genes(df))

            write_table(conn, raw_table_name(name), df)
            loaded_tables.append(name)

            metadata_rows.append({
                "table_name": raw_table_name(name),
                "display_name": source.get("display_name", name),
                "description": source.get("description", ""),
                "data_type": source.get("data_type", ""),
                "source_type": source.get("source_type", ""),
                "gene_column": "gene" if "gene" in df.columns else "",
                "unique_per_gene": source.get("unique_per_gene", True),
                "include_in_search": source.get("include_in_search", True),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            })

            logger.info(f"  Loaded {name}: {len(df)} rows, {len(df.columns)} columns")

        except Exception as e:
            logger.error(f"  Failed to load {name}: {e}")
            continue

    return loaded_tables, all_genes, metadata_rows


def _load_and_transform(source: dict, data_dir: Path | None) -> pd.DataFrame:
    """Load a source and apply transformations + column cleaning."""
    df = load_source(source, data_dir=data_dir)
    transformations = source.get("transformations", [])
    if transformations:
        df = apply_transformations(df, transformations)
    df = clean_for_db(df)
    return df


def _collect_genes(df: pd.DataFrame) -> set[str]:
    """Extract unique gene symbols from a DataFrame.

    Checks both ``gene`` and ``gene_symbol`` columns so that sources
    using either naming convention contribute to annotation lookup.
    """
    genes: set[str] = set()
    for col in ("gene", "gene_symbol"):
        if col in df.columns:
            genes.update(
                str(g) for g in df[col].dropna().unique() if g and str(g) != "nan"
            )
    return genes


