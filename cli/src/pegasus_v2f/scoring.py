"""Integration scoring — criteria count method for gene prioritization."""

from __future__ import annotations

import logging
from typing import Any

from pegasus_v2f.db import is_postgres

logger = logging.getLogger(__name__)


def compute_locus_gene_scores(conn: Any, config: dict) -> int:
    """Compute integration scores for all locus-gene pairs.

    Populates locus_gene_scores table. Returns number of rows written.
    """
    integration = config.get("pegasus", {}).get("integration", {})
    method = integration.get("method", "criteria_count_v1")
    effector_threshold = float(integration.get("effector_threshold", 0.25))
    criteria = integration.get("criteria", [])

    # Clear existing scores
    conn.execute("DELETE FROM locus_gene_scores")
    if is_postgres(conn):
        conn.commit()

    # Get all loci
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT locus_id, chromosome, start_position, end_position, "
            "lead_variant_id, lead_rsid FROM loci"
        )
        loci = cur.fetchall()
        cur.close()
    else:
        loci = conn.execute(
            "SELECT locus_id, chromosome, start_position, end_position, "
            "lead_variant_id, lead_rsid FROM loci"
        ).fetchall()

    if not loci:
        logger.warning("No loci found — skipping scoring")
        return 0

    total_rows = 0

    for locus_row in loci:
        locus_id = locus_row[0]
        locus_chr = locus_row[1]
        locus_start = locus_row[2]
        locus_end = locus_row[3]
        lead_pos = (locus_start + locus_end) // 2  # midpoint as proxy

        # Collect candidate genes from evidence tables
        genes = _get_candidate_genes(conn, locus_id)
        if not genes:
            continue

        scores = []
        for gene_symbol in genes:
            # Distance to locus midpoint
            gene_pos = _get_gene_midpoint(conn, gene_symbol)
            if gene_pos is not None:
                distance_kb = abs(gene_pos - lead_pos) / 1000.0
                is_within = _gene_overlaps_locus(
                    conn, gene_symbol, locus_chr, locus_start, locus_end
                )
            else:
                distance_kb = None
                is_within = None

            # Count criteria met
            criteria_met = _criteria_count_v1(
                conn, locus_id, gene_symbol, criteria
            )

            # Total evidence types
            n_evidence = _count_evidence_types(conn, locus_id, gene_symbol)

            # Integration score = criteria_met + normalized evidence count
            integration_score = criteria_met + (n_evidence * 0.1)

            scores.append({
                "locus_id": locus_id,
                "gene_symbol": gene_symbol,
                "distance_to_lead_kb": distance_kb,
                "is_within_locus": is_within,
                "integration_method": method,
                "integration_score": integration_score,
                "criteria_met": criteria_met,
            })

        if not scores:
            continue

        # Rank within locus (higher score = better rank)
        scores.sort(key=lambda s: -(s["integration_score"] or 0))
        for rank, s in enumerate(scores, 1):
            s["integration_rank"] = rank

        # Mark nearest gene
        scored_with_distance = [s for s in scores if s["distance_to_lead_kb"] is not None]
        if scored_with_distance:
            nearest = min(scored_with_distance, key=lambda s: s["distance_to_lead_kb"])
            nearest["is_nearest_gene"] = True

        # Mark predicted effectors
        if scores:
            max_score = scores[0]["integration_score"]
            if max_score > 0:
                for s in scores:
                    s["is_predicted_effector"] = (
                        s["integration_score"] / max_score >= effector_threshold
                    )

        # Write to locus_gene_scores
        for s in scores:
            _insert_score(conn, s)
            total_rows += 1

    if is_postgres(conn):
        conn.commit()

    logger.info(f"Computed scores for {total_rows} locus-gene pairs")
    return total_rows


def _get_candidate_genes(conn: Any, locus_id: str) -> list[str]:
    """Get all candidate genes for a locus from evidence tables."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT gene_symbol FROM locus_gene_evidence "
            "WHERE locus_id = %s AND gene_symbol != ''",
            (locus_id,),
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            "SELECT DISTINCT gene_symbol FROM locus_gene_evidence "
            "WHERE locus_id = ? AND gene_symbol != ''",
            [locus_id],
        ).fetchall()
    return [r[0] for r in rows]


def _get_gene_midpoint(conn: Any, gene_symbol: str) -> int | None:
    """Get midpoint of a gene from the genes table."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT start_position, end_position FROM genes WHERE gene_symbol = %s",
            (gene_symbol,),
        )
        row = cur.fetchone()
        cur.close()
    else:
        row = conn.execute(
            "SELECT start_position, end_position FROM genes WHERE gene_symbol = ?",
            [gene_symbol],
        ).fetchone()

    if row and row[0] is not None and row[1] is not None:
        return (row[0] + row[1]) // 2
    return None


def _gene_overlaps_locus(
    conn: Any, gene_symbol: str,
    locus_chr: str, locus_start: int, locus_end: int,
) -> bool | None:
    """Check if a gene's coordinates overlap a locus boundary."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT chromosome, start_position, end_position FROM genes WHERE gene_symbol = %s",
            (gene_symbol,),
        )
        row = cur.fetchone()
        cur.close()
    else:
        row = conn.execute(
            "SELECT chromosome, start_position, end_position FROM genes WHERE gene_symbol = ?",
            [gene_symbol],
        ).fetchone()

    if not row or row[1] is None:
        return None
    gene_chr, gene_start, gene_end = str(row[0]), row[1], row[2]
    if gene_chr != str(locus_chr):
        return False
    return gene_start <= locus_end and gene_end >= locus_start


def _criteria_count_v1(
    conn: Any, locus_id: str, gene_symbol: str, criteria: list[dict],
) -> int:
    """Count how many integration criteria a gene meets at a locus."""
    met = 0
    for criterion in criteria:
        crit_type = criterion.get("type", "evidence")
        if crit_type == "computed":
            # Computed criteria (like nearest_gene) are handled post-hoc
            continue

        category = criterion.get("category")
        threshold_field = criterion.get("threshold_field", "score")
        threshold = criterion.get("threshold")

        if not category:
            continue

        # Check if there's evidence in this category meeting the threshold
        if is_postgres(conn):
            cur = conn.cursor()
            if threshold is not None:
                cur.execute(
                    f"SELECT 1 FROM locus_gene_evidence "
                    f"WHERE locus_id = %s AND gene_symbol = %s AND evidence_category = %s "
                    f"AND {threshold_field} >= %s LIMIT 1",
                    (locus_id, gene_symbol, category, threshold),
                )
            else:
                cur.execute(
                    "SELECT 1 FROM locus_gene_evidence "
                    "WHERE locus_id = %s AND gene_symbol = %s AND evidence_category = %s LIMIT 1",
                    (locus_id, gene_symbol, category),
                )
            found = cur.fetchone() is not None
            cur.close()
        else:
            if threshold is not None:
                found = conn.execute(
                    f"SELECT 1 FROM locus_gene_evidence "
                    f"WHERE locus_id = ? AND gene_symbol = ? AND evidence_category = ? "
                    f"AND {threshold_field} >= ? LIMIT 1",
                    [locus_id, gene_symbol, category, threshold],
                ).fetchone() is not None
            else:
                found = conn.execute(
                    "SELECT 1 FROM locus_gene_evidence "
                    "WHERE locus_id = ? AND gene_symbol = ? AND evidence_category = ? LIMIT 1",
                    [locus_id, gene_symbol, category],
                ).fetchone() is not None

        if found:
            met += 1

    return met


def _count_evidence_types(conn: Any, locus_id: str, gene_symbol: str) -> int:
    """Count distinct evidence categories for a gene at a locus."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT evidence_category) FROM locus_gene_evidence "
            "WHERE locus_id = %s AND gene_symbol = %s",
            (locus_id, gene_symbol),
        )
        row = cur.fetchone()
        cur.close()
    else:
        row = conn.execute(
            "SELECT COUNT(DISTINCT evidence_category) FROM locus_gene_evidence "
            "WHERE locus_id = ? AND gene_symbol = ?",
            [locus_id, gene_symbol],
        ).fetchone()

    # Also count gene-level evidence
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT evidence_category) FROM gene_evidence "
            "WHERE gene_symbol = %s",
            (gene_symbol,),
        )
        gene_row = cur.fetchone()
        cur.close()
    else:
        gene_row = conn.execute(
            "SELECT COUNT(DISTINCT evidence_category) FROM gene_evidence "
            "WHERE gene_symbol = ?",
            [gene_symbol],
        ).fetchone()

    return (row[0] if row else 0) + (gene_row[0] if gene_row else 0)


def _insert_score(conn: Any, s: dict) -> None:
    """Insert a single score row."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO locus_gene_scores "
            "(locus_id, gene_symbol, distance_to_lead_kb, is_nearest_gene, "
            "is_within_locus, integration_method, integration_score, "
            "integration_rank, is_predicted_effector) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (locus_id, gene_symbol) DO UPDATE SET "
            "integration_score = EXCLUDED.integration_score, "
            "integration_rank = EXCLUDED.integration_rank",
            (
                s["locus_id"], s["gene_symbol"],
                s.get("distance_to_lead_kb"),
                s.get("is_nearest_gene", False),
                s.get("is_within_locus"),
                s.get("integration_method"),
                s.get("integration_score"),
                s.get("integration_rank"),
                s.get("is_predicted_effector", False),
            ),
        )
        cur.close()
    else:
        conn.execute(
            "INSERT OR REPLACE INTO locus_gene_scores "
            "(locus_id, gene_symbol, distance_to_lead_kb, is_nearest_gene, "
            "is_within_locus, integration_method, integration_score, "
            "integration_rank, is_predicted_effector) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                s["locus_id"], s["gene_symbol"],
                s.get("distance_to_lead_kb"),
                s.get("is_nearest_gene", False),
                s.get("is_within_locus"),
                s.get("integration_method"),
                s.get("integration_score"),
                s.get("integration_rank"),
                s.get("is_predicted_effector", False),
            ],
        )
