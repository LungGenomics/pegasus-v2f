"""PEGASUS export — evidence matrix, metadata YAML, and PEG list."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pegasus_v2f.db import is_postgres

logger = logging.getLogger(__name__)


def export_evidence_matrix(conn: Any, study_id: str, output_dir: Path) -> Path:
    """Export PEGASUS evidence matrix as TSV.

    Pivots locus_gene_evidence + gene_evidence into a wide-format matrix
    where rows are locus-gene pairs and columns are evidence types.

    Returns path to written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get all loci for this study
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT locus_id, locus_name, chromosome, start_position, end_position "
            "FROM loci WHERE study_id = %s ORDER BY chromosome, start_position",
            (study_id,),
        )
        loci = cur.fetchall()
        cur.close()
    else:
        loci = conn.execute(
            "SELECT locus_id, locus_name, chromosome, start_position, end_position "
            "FROM loci WHERE study_id = ? ORDER BY chromosome, start_position",
            [study_id],
        ).fetchall()

    if not loci:
        logger.warning(f"No loci found for study {study_id}")
        out_path = output_dir / "evidence_matrix.tsv"
        pd.DataFrame(columns=["locus_id", "locus_name", "chromosome", "start", "end", "gene_symbol"]).to_csv(
            out_path, sep="\t", index=False
        )
        return out_path

    locus_ids = [l[0] for l in loci]

    # Build locus-gene evidence rows
    placeholders = ",".join(["?" for _ in locus_ids])
    if is_postgres(conn):
        placeholders = ",".join(["%s" for _ in locus_ids])
        cur = conn.cursor()
        cur.execute(
            f"SELECT locus_id, gene_symbol, evidence_category, evidence_stream, "
            f"pvalue, score, tissue "
            f"FROM locus_gene_evidence WHERE locus_id IN ({placeholders})",
            tuple(locus_ids),
        )
        lge_rows = cur.fetchall()
        cur.close()
    else:
        lge_rows = conn.execute(
            f"SELECT locus_id, gene_symbol, evidence_category, evidence_stream, "
            f"pvalue, score, tissue "
            f"FROM locus_gene_evidence WHERE locus_id IN ({placeholders})",
            locus_ids,
        ).fetchall()

    # Collect unique evidence column names
    evidence_cols = set()
    for row in lge_rows:
        category = row[2]
        stream = row[3] or ""
        col_name = f"{category}_{stream}" if stream else category
        evidence_cols.add(col_name)

    evidence_cols = sorted(evidence_cols)

    # Build matrix
    matrix_rows = []
    locus_lookup = {l[0]: l for l in loci}

    # Group evidence by (locus_id, gene_symbol)
    evidence_map: dict[tuple[str, str], dict] = {}
    for row in lge_rows:
        locus_id, gene, category, stream = row[0], row[1], row[2], row[3] or ""
        pvalue, score, tissue = row[4], row[5], row[6]
        key = (locus_id, gene)
        if key not in evidence_map:
            evidence_map[key] = {}
        col_name = f"{category}_{stream}" if stream else category
        # Use score if available, else pvalue, else 1 (present)
        value = score if score is not None else (pvalue if pvalue is not None else 1)
        evidence_map[key][col_name] = value

    for (locus_id, gene), ev_dict in evidence_map.items():
        locus = locus_lookup.get(locus_id)
        if not locus:
            continue
        row_dict = {
            "locus_id": locus_id,
            "locus_name": locus[1],
            "chromosome": locus[2],
            "start": locus[3],
            "end": locus[4],
            "gene_symbol": gene,
        }
        for col in evidence_cols:
            row_dict[col] = ev_dict.get(col, "")
        matrix_rows.append(row_dict)

    df = pd.DataFrame(matrix_rows)
    if len(df) == 0:
        df = pd.DataFrame(columns=["locus_id", "locus_name", "chromosome", "start", "end", "gene_symbol"])

    out_path = output_dir / "evidence_matrix.tsv"
    df.to_csv(out_path, sep="\t", index=False)
    logger.info(f"Evidence matrix: {len(df)} rows, {len(evidence_cols)} evidence columns → {out_path}")
    return out_path


def export_metadata(conn: Any, study_id: str, output_dir: Path) -> Path:
    """Export PEGASUS metadata YAML.

    Contains study info, loci summary, evidence categories used, and data sources.

    Returns path to written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Study info
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM studies WHERE study_id = %s", (study_id,))
        cols = [desc[0] for desc in cur.description]
        study_row = cur.fetchone()
        cur.close()
    else:
        result = conn.execute(
            "SELECT * FROM studies WHERE study_id = ?", [study_id]
        )
        cols = [desc[0] for desc in result.description]
        study_row = result.fetchone()

    if not study_row:
        logger.warning(f"Study {study_id} not found")
        out_path = output_dir / "metadata.yaml"
        out_path.write_text("")
        return out_path

    study_dict = dict(zip(cols, study_row))

    # Loci count
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM loci WHERE study_id = %s", (study_id,))
        n_loci = cur.fetchone()[0]
        cur.close()
    else:
        n_loci = conn.execute(
            "SELECT COUNT(*) FROM loci WHERE study_id = ?", [study_id]
        ).fetchone()[0]

    # Evidence categories used
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT evidence_category FROM locus_gene_evidence lge "
            "JOIN loci l ON lge.locus_id = l.locus_id WHERE l.study_id = %s",
            (study_id,),
        )
        categories = sorted(r[0] for r in cur.fetchall())
        cur.close()
    else:
        categories = sorted(
            r[0] for r in conn.execute(
                "SELECT DISTINCT evidence_category FROM locus_gene_evidence lge "
                "JOIN loci l ON lge.locus_id = l.locus_id WHERE l.study_id = ?",
                [study_id],
            ).fetchall()
        )

    # Data sources
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT source_tag, source_name, evidence_category FROM data_sources "
            "WHERE is_integrated = TRUE"
        )
        ds_rows = cur.fetchall()
        cur.close()
    else:
        ds_rows = conn.execute(
            "SELECT source_tag, source_name, evidence_category FROM data_sources "
            "WHERE is_integrated = TRUE"
        ).fetchall()

    metadata = {
        "study": {k: v for k, v in study_dict.items() if v is not None},
        "n_loci": n_loci,
        "evidence_categories": categories,
        "data_sources": [
            {"source_tag": r[0], "source_name": r[1], "evidence_category": r[2]}
            for r in ds_rows
        ],
    }

    out_path = output_dir / "metadata.yaml"
    out_path.write_text(yaml.dump(metadata, default_flow_style=False, sort_keys=False))
    logger.info(f"Metadata → {out_path}")
    return out_path


def export_peg_list(conn: Any, study_id: str, output_dir: Path) -> Path:
    """Export PEG list — rank-1 predicted effector gene per locus.

    Returns path to written file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
            "s.gene_symbol, s.integration_score, s.integration_rank, s.is_predicted_effector "
            "FROM locus_gene_scores s "
            "JOIN loci l ON s.locus_id = l.locus_id "
            "WHERE l.study_id = %s AND s.integration_rank = 1 "
            "ORDER BY l.chromosome, l.start_position",
            (study_id,),
        )
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
    else:
        result = conn.execute(
            "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
            "s.gene_symbol, s.integration_score, s.integration_rank, s.is_predicted_effector "
            "FROM locus_gene_scores s "
            "JOIN loci l ON s.locus_id = l.locus_id "
            "WHERE l.study_id = ? AND s.integration_rank = 1 "
            "ORDER BY l.chromosome, l.start_position",
            [study_id],
        )
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    out_path = output_dir / "peg_list.tsv"
    df.to_csv(out_path, sep="\t", index=False)
    logger.info(f"PEG list: {len(df)} loci → {out_path}")
    return out_path


def export_all(conn: Any, study_id: str, output_dir: Path) -> dict[str, Path]:
    """Export all three PEGASUS deliverables.

    Returns dict of deliverable name → file path.
    """
    return {
        "evidence_matrix": export_evidence_matrix(conn, study_id, output_dir),
        "metadata": export_metadata(conn, study_id, output_dir),
        "peg_list": export_peg_list(conn, study_id, output_dir),
    }
