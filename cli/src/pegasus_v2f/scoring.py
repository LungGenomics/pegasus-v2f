"""Materialization and scoring — join evidence × loci × genes into scored_evidence."""

from __future__ import annotations

import logging
from typing import Any

from pegasus_v2f.db import is_postgres

logger = logging.getLogger(__name__)


def materialize_scored_evidence(
    conn: Any,
    config: dict,
    study_name: str | None = None,
) -> int:
    """Materialize scored_evidence from evidence × loci × genes.

    For each locus:
      1. Find candidate genes (gene coords overlap locus window) → match_type='gene'
      2. Find variant-level evidence (chr/pos in locus window) → match_type='position'
      3. Find gene-level evidence (gene_symbol in candidates) → match_type='gene'
      4. Score and rank genes within each locus

    Args:
        conn: Open database connection.
        config: Full config dict.
        study_name: If set, only materialize for this study's loci.

    Returns:
        Number of scored_evidence rows written.
    """
    # Clear scored_evidence (all or for target study)
    if study_name:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM scored_evidence WHERE study_id IN "
                "(SELECT study_id FROM studies WHERE study_name = %s)",
                (study_name,),
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                "DELETE FROM scored_evidence WHERE study_id IN "
                "(SELECT study_id FROM studies WHERE study_name = ?)",
                [study_name],
            )
    else:
        conn.execute("DELETE FROM scored_evidence")
        if is_postgres(conn):
            conn.commit()

    # Ensure genes table has data (candidate gene enumeration needs coordinates)
    try:
        gene_count = conn.execute("SELECT COUNT(*) FROM genes").fetchone()[0]
        if gene_count == 0:
            logger.info("genes table is empty — fetching annotations")
            from pegasus_v2f.annotate import create_gene_annotations
            # Collect all gene symbols from evidence
            rows = conn.execute(
                "SELECT DISTINCT gene_symbol FROM evidence"
            ).fetchall()
            all_genes = [r[0] for r in rows if r[0]]
            if all_genes:
                create_gene_annotations(conn, all_genes, config)
                logger.info(f"Fetched annotations for {len(all_genes)} genes")
    except Exception as e:
        logger.warning(f"Could not check/fetch gene annotations: {e}")

    # Get loci (all or for target study)
    loci = _get_loci(conn, study_name)
    if not loci:
        logger.warning("No loci found — skipping scoring")
        return 0

    total_rows = 0

    for locus in loci:
        locus_id = locus["locus_id"]
        study_id = locus["study_id"]
        locus_chr = locus["chromosome"]
        locus_start = locus["start_position"]
        locus_end = locus["end_position"]

        # 1. Candidate genes by geometry (gene coords overlap locus window)
        candidate_genes = _get_candidate_genes_by_geometry(
            conn, locus_chr, locus_start, locus_end
        )

        # 2. Variant-level evidence in locus window
        variant_evidence = _get_variant_evidence_in_window(
            conn, locus_chr, locus_start, locus_end
        )

        # 3. Gene-level evidence for candidate genes + any genes from variant evidence
        all_gene_symbols = set(candidate_genes)
        for ev in variant_evidence:
            all_gene_symbols.add(ev["gene_symbol"])

        gene_evidence = _get_gene_level_evidence(conn, all_gene_symbols)

        # Collect all evidence rows for this locus
        evidence_rows = []

        # Variant evidence → match_type='position'
        for ev in variant_evidence:
            evidence_rows.append({**ev, "match_type": "position"})

        # Gene evidence → match_type='gene'
        for ev in gene_evidence:
            evidence_rows.append({**ev, "match_type": "gene"})

        # Group by gene, count categories, compute rank
        gene_scores = _score_genes(evidence_rows, candidate_genes)
        n_candidates = len(set(candidate_genes) | {ev["gene_symbol"] for ev in evidence_rows})

        # Write scored_evidence rows
        for ev in evidence_rows:
            gene = ev["gene_symbol"]
            gs = gene_scores.get(gene, {})
            _insert_scored_evidence(
                conn,
                locus_id=locus_id,
                study_id=study_id,
                evidence=ev,
                integration_rank=gs.get("rank"),
                is_predicted_effector=gs.get("is_predicted_effector", False),
                n_candidate_genes=n_candidates,
            )
            total_rows += 1

        # Also write rows for candidate genes with NO evidence
        genes_with_evidence = {ev["gene_symbol"] for ev in evidence_rows}
        for gene in candidate_genes:
            if gene not in genes_with_evidence:
                _insert_scored_evidence(
                    conn,
                    locus_id=locus_id,
                    study_id=study_id,
                    evidence={"gene_symbol": gene, "match_type": "gene"},
                    integration_rank=gene_scores.get(gene, {}).get("rank"),
                    is_predicted_effector=False,
                    n_candidate_genes=n_candidates,
                )
                total_rows += 1

    # Update candidate gene counts on loci table
    conn.execute("""
        UPDATE loci SET n_candidate_genes = (
            SELECT COUNT(DISTINCT gene_symbol) FROM scored_evidence
            WHERE scored_evidence.locus_id = loci.locus_id
        )
    """)

    if is_postgres(conn):
        conn.commit()

    # Rebuild gene search index
    try:
        from pegasus_v2f.annotate import create_pegasus_search_index
        create_pegasus_search_index(conn)
        logger.info("Rebuilt gene_search_index")
    except Exception as e:
        logger.warning(f"Could not rebuild gene_search_index: {e}")

    logger.info(f"Materialized {total_rows} scored_evidence rows")
    return total_rows


def _get_loci(conn: Any, study_name: str | None) -> list[dict]:
    """Get loci, optionally filtered by study_name."""
    if study_name:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT l.locus_id, l.study_id, l.chromosome, l.start_position, l.end_position "
                "FROM loci l JOIN studies s ON l.study_id = s.study_id "
                "WHERE s.study_name = %s",
                (study_name,),
            )
            rows = cur.fetchall()
            cur.close()
        else:
            rows = conn.execute(
                "SELECT l.locus_id, l.study_id, l.chromosome, l.start_position, l.end_position "
                "FROM loci l JOIN studies s ON l.study_id = s.study_id "
                "WHERE s.study_name = ?",
                [study_name],
            ).fetchall()
    else:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT locus_id, study_id, chromosome, start_position, end_position FROM loci"
            )
            rows = cur.fetchall()
            cur.close()
        else:
            rows = conn.execute(
                "SELECT locus_id, study_id, chromosome, start_position, end_position FROM loci"
            ).fetchall()

    return [
        {
            "locus_id": r[0],
            "study_id": r[1],
            "chromosome": r[2],
            "start_position": r[3],
            "end_position": r[4],
        }
        for r in rows
    ]


def _get_candidate_genes_by_geometry(
    conn: Any, chromosome: str, start: int, end: int,
) -> list[str]:
    """Find genes whose coordinates overlap the locus window."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT gene_symbol FROM genes "
            "WHERE chromosome = %s AND start_position <= %s AND end_position >= %s",
            (str(chromosome), end, start),
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            "SELECT gene_symbol FROM genes "
            "WHERE chromosome = ? AND start_position <= ? AND end_position >= ?",
            [str(chromosome), end, start],
        ).fetchall()

    return [r[0] for r in rows]


def _get_variant_evidence_in_window(
    conn: Any, chromosome: str, start: int, end: int,
) -> list[dict]:
    """Get variant-level evidence (has chr/pos) within the locus window."""
    cols = (
        "gene_symbol, evidence_category, source_tag, trait, pvalue, "
        "effect_size, score, tissue, cell_type, rsid, ancestry, sex"
    )
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM evidence "
            "WHERE chromosome = %s AND position >= %s AND position <= %s",
            (str(chromosome), start, end),
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            f"SELECT {cols} FROM evidence "
            "WHERE chromosome = ? AND position >= ? AND position <= ?",
            [str(chromosome), start, end],
        ).fetchall()

    return [_evidence_row_to_dict(r) for r in rows]


def _get_gene_level_evidence(conn: Any, gene_symbols: set[str]) -> list[dict]:
    """Get gene-level evidence (no chr/pos) for a set of gene symbols."""
    if not gene_symbols:
        return []

    cols = (
        "gene_symbol, evidence_category, source_tag, trait, pvalue, "
        "effect_size, score, tissue, cell_type, rsid, ancestry, sex"
    )

    # Build IN clause
    placeholders = ", ".join(["?"] * len(gene_symbols))
    genes_list = list(gene_symbols)

    if is_postgres(conn):
        placeholders = ", ".join(["%s"] * len(gene_symbols))
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM evidence "
            f"WHERE chromosome IS NULL AND gene_symbol IN ({placeholders})",
            genes_list,
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            f"SELECT {cols} FROM evidence "
            f"WHERE chromosome IS NULL AND gene_symbol IN ({placeholders})",
            genes_list,
        ).fetchall()

    return [_evidence_row_to_dict(r) for r in rows]


def _evidence_row_to_dict(row: tuple) -> dict:
    """Convert a raw evidence query row to a dict."""
    return {
        "gene_symbol": row[0],
        "evidence_category": row[1],
        "source_tag": row[2],
        "trait": row[3],
        "pvalue": row[4],
        "effect_size": row[5],
        "score": row[6],
        "tissue": row[7],
        "cell_type": row[8],
        "rsid": row[9],
        "ancestry": row[10],
        "sex": row[11],
    }


def _score_genes(evidence_rows: list[dict], candidate_genes: list[str]) -> dict:
    """Score genes by counting distinct evidence categories.

    Returns dict of gene_symbol → {score, rank, is_predicted_effector}.
    """
    # Count distinct evidence categories per gene
    gene_categories: dict[str, set[str]] = {}
    all_genes = set(candidate_genes)

    for ev in evidence_rows:
        gene = ev["gene_symbol"]
        all_genes.add(gene)
        cat = ev.get("evidence_category")
        if cat:
            gene_categories.setdefault(gene, set()).add(cat)

    # Score = number of distinct categories
    scored = []
    for gene in all_genes:
        cats = gene_categories.get(gene, set())
        scored.append({"gene_symbol": gene, "score": len(cats)})

    # Rank (higher score = rank 1)
    scored.sort(key=lambda s: -s["score"])
    result = {}
    for rank, s in enumerate(scored, 1):
        is_effector = False
        if scored[0]["score"] > 0:
            is_effector = s["score"] / scored[0]["score"] >= 0.25
        result[s["gene_symbol"]] = {
            "rank": rank,
            "is_predicted_effector": is_effector,
        }

    return result


def _insert_scored_evidence(
    conn: Any, *,
    locus_id: str,
    study_id: str,
    evidence: dict,
    integration_rank: int | None,
    is_predicted_effector: bool,
    n_candidate_genes: int,
) -> None:
    """Insert a single scored_evidence row."""
    values = [
        locus_id,
        study_id,
        evidence["gene_symbol"],
        evidence.get("evidence_category"),
        evidence.get("source_tag"),
        evidence.get("trait"),
        evidence.get("pvalue"),
        evidence.get("effect_size"),
        evidence.get("score"),
        evidence.get("tissue"),
        evidence.get("cell_type"),
        evidence.get("rsid"),
        evidence.get("ancestry"),
        evidence.get("sex"),
        evidence.get("match_type"),
        integration_rank,
        is_predicted_effector,
        n_candidate_genes,
    ]

    cols = (
        "locus_id, study_id, gene_symbol, evidence_category, source_tag, "
        "trait, pvalue, effect_size, score, tissue, cell_type, rsid, "
        "ancestry, sex, match_type, integration_rank, is_predicted_effector, "
        "n_candidate_genes"
    )

    if is_postgres(conn):
        placeholders = ", ".join(["%s"] * len(values))
        cur = conn.cursor()
        cur.execute(f"INSERT INTO scored_evidence ({cols}) VALUES ({placeholders})", values)
        cur.close()
    else:
        placeholders = ", ".join(["?"] * len(values))
        conn.execute(f"INSERT INTO scored_evidence ({cols}) VALUES ({placeholders})", values)
