"""Unified evidence loader — writes to the evidence table only.

No locus matching, no study dependency. Evidence is independent and
connected to loci only at score time via scored_evidence.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from pegasus_v2f.db import is_postgres
from pegasus_v2f.evidence_config import resolve_evidence_mapping

logger = logging.getLogger(__name__)


def load_evidence(conn: Any, source: dict, df: pd.DataFrame, evidence_block: dict) -> dict:
    """Load a single evidence block into the evidence table.

    Args:
        conn: Open database connection.
        source: Source config dict (name, source_type, etc.).
        df: DataFrame with raw data.
        evidence_block: Single evidence config dict (category, source_tag, fields, etc.).

    Returns:
        Summary dict with rows_inserted, category, source_tag.
    """
    source_tag = evidence_block["source_tag"]
    category = evidence_block["category"]

    # Resolve column mappings
    src_view = {**source, "evidence": evidence_block}
    mapping = resolve_evidence_mapping(src_view, df)

    # Clean up old evidence for this source_tag
    _cleanup_evidence(conn, source_tag)

    # Determine if variant-level (has chr/pos) or gene-level
    chr_col = mapping.get("chromosome")
    pos_col = mapping.get("position")
    gene_col = mapping["gene"]
    pval_col = mapping.get("pvalue")
    score_col = mapping.get("score")
    effect_col = mapping.get("effect_size")
    tissue_col = mapping.get("tissue")
    cell_type_col = mapping.get("cell_type")
    rsid_col = mapping.get("rsid")
    ancestry_col = mapping.get("ancestry")
    sex_col = mapping.get("sex")
    stream_col = mapping.get("evidence_stream")

    # Trait handling: source-level traits list or per-row trait column
    trait_col = mapping.get("trait")
    source_traits = evidence_block.get("traits")  # list of trait tags

    working = df.copy()

    # Build trait series
    if trait_col:
        working["__trait__"] = working[trait_col].astype(str)
    elif source_traits:
        working["__trait__"] = ", ".join(source_traits)
    else:
        working["__trait__"] = None

    rows = []
    for _, row in working.iterrows():
        gene = str(row[gene_col])
        if not gene or gene == "nan":
            continue

        evidence_row = {
            "gene_symbol": gene,
            "evidence_category": category,
            "source_tag": source_tag,
            "trait": row["__trait__"] if pd.notna(row.get("__trait__")) else None,
        }

        # Variant-level fields
        if chr_col and pos_col:
            chrom_val = row[chr_col]
            pos_val = row[pos_col]
            if pd.notna(chrom_val) and pd.notna(pos_val):
                evidence_row["chromosome"] = str(chrom_val)
                try:
                    evidence_row["position"] = int(float(pos_val))
                except (ValueError, TypeError):
                    pass

        # Optional fields
        if rsid_col and pd.notna(row.get(rsid_col)):
            evidence_row["rsid"] = str(row[rsid_col])
        if pval_col and pd.notna(row.get(pval_col)):
            try:
                evidence_row["pvalue"] = float(row[pval_col])
            except (ValueError, TypeError):
                pass
        if effect_col and pd.notna(row.get(effect_col)):
            try:
                evidence_row["effect_size"] = float(row[effect_col])
            except (ValueError, TypeError):
                pass
        if score_col and pd.notna(row.get(score_col)):
            try:
                evidence_row["score"] = float(row[score_col])
            except (ValueError, TypeError):
                pass
        if tissue_col and pd.notna(row.get(tissue_col)):
            evidence_row["tissue"] = str(row[tissue_col])
        if cell_type_col and pd.notna(row.get(cell_type_col)):
            evidence_row["cell_type"] = str(row[cell_type_col])
        if ancestry_col and pd.notna(row.get(ancestry_col)):
            evidence_row["ancestry"] = str(row[ancestry_col])
        if sex_col and pd.notna(row.get(sex_col)):
            evidence_row["sex"] = str(row[sex_col])
        if stream_col and pd.notna(row.get(stream_col)):
            evidence_row["evidence_stream"] = str(row[stream_col])

        rows.append(evidence_row)

    # Bulk insert
    inserted = _bulk_insert_evidence(conn, rows)

    # Update data_sources provenance
    _upsert_data_source(
        conn, source_tag, source.get("name", source_tag),
        source_type=source.get("source_type"),
        evidence_category=category,
        record_count=inserted,
        url=source.get("url"),
    )

    logger.info(f"Evidence loaded: {inserted} rows ({category}, source_tag={source_tag})")
    return {
        "rows_inserted": inserted,
        "category": category,
        "source_tag": source_tag,
    }


def load_all_evidence(conn: Any, source: dict, df: pd.DataFrame) -> list[dict]:
    """Load all evidence blocks from a source config.

    Iterates over source["evidence"] (always a list) and calls load_evidence
    for each block.

    Returns list of summary dicts.
    """
    evidence_blocks = source.get("evidence") or []
    if not evidence_blocks:
        return []

    results = []
    for block in evidence_blocks:
        result = load_evidence(conn, source, df, block)
        results.append(result)

    return results


def _cleanup_evidence(conn: Any, source_tag: str) -> None:
    """Remove evidence rows from a previous load of this source_tag."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("DELETE FROM evidence WHERE source_tag = %s", (source_tag,))
        conn.commit()
        cur.close()
    else:
        conn.execute("DELETE FROM evidence WHERE source_tag = ?", [source_tag])
    logger.debug(f"Cleaned up previous evidence for source_tag '{source_tag}'")


def _bulk_insert_evidence(conn: Any, rows: list[dict]) -> int:
    """Insert evidence rows into the evidence table. Returns count inserted."""
    if not rows:
        return 0

    columns = [
        "gene_symbol", "chromosome", "position", "rsid",
        "evidence_category", "source_tag", "trait",
        "pvalue", "effect_size", "score",
        "tissue", "cell_type", "ancestry", "sex",
        "evidence_stream", "is_supporting",
    ]

    inserted = 0
    for row in rows:
        values = [row.get(col) for col in columns]
        try:
            if is_postgres(conn):
                placeholders = ", ".join(["%s"] * len(columns))
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO evidence ({', '.join(columns)}) VALUES ({placeholders})",
                    values,
                )
                conn.commit()
                cur.close()
            else:
                placeholders = ", ".join(["?"] * len(columns))
                conn.execute(
                    f"INSERT INTO evidence ({', '.join(columns)}) VALUES ({placeholders})",
                    values,
                )
            inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert evidence row: {e}")

    return inserted


def _upsert_data_source(
    conn: Any, source_tag: str, source_name: str, *,
    source_type: str | None = None,
    evidence_category: str | None = None,
    record_count: int | None = None,
    url: str | None = None,
) -> None:
    """Insert or update a row in the data_sources provenance table."""
    now = datetime.now(timezone.utc).isoformat()
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO data_sources
                (source_tag, source_name, source_type, evidence_category,
                 is_integrated, date_imported, record_count, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_tag) DO UPDATE SET
                source_name = EXCLUDED.source_name,
                record_count = EXCLUDED.record_count,
                date_imported = EXCLUDED.date_imported
            """,
            (source_tag, source_name, source_type, evidence_category,
             True, now, record_count, url),
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            """INSERT OR REPLACE INTO data_sources
                (source_tag, source_name, source_type, evidence_category,
                 is_integrated, date_imported, record_count, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [source_tag, source_name, source_type, evidence_category,
             True, now, record_count, url],
        )
