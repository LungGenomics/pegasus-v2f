"""PEGASUS export — evidence matrix, metadata YAML, and PEG list."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pegasus_v2f.db import is_postgres

logger = logging.getLogger(__name__)


def _resolve_study_ids(conn: Any, study_name: str) -> list[str]:
    """Resolve a study_name to its study_ids. Also accepts a direct study_id."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM studies WHERE study_name = %s", (study_name,))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            "SELECT study_id FROM studies WHERE study_name = ?", [study_name]
        ).fetchall()

    if rows:
        return [r[0] for r in rows]

    # Fall back: maybe they passed a direct study_id
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM studies WHERE study_id = %s", (study_name,))
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            "SELECT study_id FROM studies WHERE study_id = ?", [study_name]
        ).fetchall()

    return [r[0] for r in rows]


def _in_clause(ids: list[str], pg: bool) -> tuple[str, list | tuple]:
    """Build an IN clause and params for a list of IDs."""
    if pg:
        return ",".join(["%s"] * len(ids)), tuple(ids)
    return ",".join(["?"] * len(ids)), ids


def export_evidence_matrix(conn: Any, study_ids: list[str], output_dir: Path) -> Path:
    """Export PEGASUS evidence matrix as TSV."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ph, params = _in_clause(study_ids, is_postgres(conn))

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT locus_id, locus_name, chromosome, start_position, end_position "
            f"FROM loci WHERE study_id IN ({ph}) ORDER BY chromosome, start_position",
            params,
        )
        loci = cur.fetchall()
        cur.close()
    else:
        loci = conn.execute(
            f"SELECT locus_id, locus_name, chromosome, start_position, end_position "
            f"FROM loci WHERE study_id IN ({ph}) ORDER BY chromosome, start_position",
            params,
        ).fetchall()

    if not loci:
        logger.warning(f"No loci found for study_ids {study_ids}")
        out_path = output_dir / "evidence_matrix.tsv"
        pd.DataFrame(columns=["locus_id", "locus_name", "chromosome", "start", "end", "gene_symbol"]).to_csv(
            out_path, sep="\t", index=False
        )
        return out_path

    locus_ids = [l[0] for l in loci]
    lph, lparams = _in_clause(locus_ids, is_postgres(conn))

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT locus_id, gene_symbol, evidence_category, source_tag, "
            f"pvalue, score, tissue "
            f"FROM scored_evidence WHERE locus_id IN ({lph})",
            lparams,
        )
        se_rows = cur.fetchall()
        cur.close()
    else:
        se_rows = conn.execute(
            f"SELECT locus_id, gene_symbol, evidence_category, source_tag, "
            f"pvalue, score, tissue "
            f"FROM scored_evidence WHERE locus_id IN ({lph})",
            lparams,
        ).fetchall()

    # Build evidence column names: Category_sourceTag (PEGASUS standard naming)
    evidence_col_keys = sorted({(row[2], row[3]) for row in se_rows if row[2]})
    locus_lookup = {l[0]: l for l in loci}

    evidence_map: dict[tuple[str, str], dict] = {}
    for row in se_rows:
        locus_id, gene, category, source_tag = row[0], row[1], row[2], row[3]
        pvalue, score = row[4], row[5]
        if not category:
            continue
        key = (locus_id, gene)
        if key not in evidence_map:
            evidence_map[key] = {}
        col_name = f"{category}_{source_tag}" if source_tag else category
        value = score if score is not None else (pvalue if pvalue is not None else 1)
        evidence_map[key][col_name] = value

    evidence_cols = sorted({col for ev_dict in evidence_map.values() for col in ev_dict})

    # Lookup tables for variant/gene IDs
    _lead_variants = {}
    for l in loci:
        _lead_variants[l[0]] = l  # locus_id -> full locus row

    # Try to get Ensembl gene IDs
    gene_ids: dict[str, str] = {}
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute("SELECT gene_symbol, ensembl_gene_id FROM genes WHERE ensembl_gene_id IS NOT NULL")
            gene_ids = {r[0]: r[1] for r in cur.fetchall()}
            cur.close()
        else:
            gene_ids = {
                r[0]: r[1] for r in conn.execute(
                    "SELECT gene_symbol, ensembl_gene_id FROM genes WHERE ensembl_gene_id IS NOT NULL"
                ).fetchall()
            }
    except Exception:
        pass  # genes table may be empty

    # Try to get lead variant info from loci
    loci_variants: dict[str, tuple] = {}
    for l in loci:
        locus_id = l[0]
        loci_variants[locus_id] = l  # full row

    matrix_rows = []
    for (locus_id, gene), ev_dict in evidence_map.items():
        locus = locus_lookup.get(locus_id)
        if not locus:
            continue
        # Format locus range as chr:start-end (PEGASUS standard)
        locus_range = f"{locus[2]}:{locus[3]}-{locus[4]}"
        row_dict = {
            "locus_id": locus_id,
            "locus_name": locus[1],
            "locus_range": locus_range,
            "chromosome": locus[2],
            "start": locus[3],
            "end": locus[4],
            "gene_symbol": gene,
            "ensembl_gene_id": gene_ids.get(gene, ""),
        }
        for col in evidence_cols:
            row_dict[col] = ev_dict.get(col, "")
        matrix_rows.append(row_dict)

    df = pd.DataFrame(matrix_rows)
    if len(df) == 0:
        df = pd.DataFrame(columns=[
            "locus_id", "locus_name", "locus_range", "chromosome", "start", "end",
            "gene_symbol", "ensembl_gene_id",
        ])

    out_path = output_dir / "evidence_matrix.tsv"
    df.to_csv(out_path, sep="\t", index=False)
    logger.info(f"Evidence matrix: {len(df)} rows, {len(evidence_cols)} evidence columns → {out_path}")
    return out_path


def export_metadata(conn: Any, study_ids: list[str], output_dir: Path) -> Path:
    """Export PEGASUS metadata YAML."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ph, params = _in_clause(study_ids, is_postgres(conn))

    # Study info (all trait rows)
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM studies WHERE study_id IN ({ph})", params)
        cols = [desc[0] for desc in cur.description]
        study_rows = cur.fetchall()
        cur.close()
    else:
        result = conn.execute(f"SELECT * FROM studies WHERE study_id IN ({ph})", params)
        cols = [desc[0] for desc in result.description]
        study_rows = result.fetchall()

    if not study_rows:
        logger.warning(f"No studies found for {study_ids}")
        out_path = output_dir / "metadata.yaml"
        out_path.write_text("")
        return out_path

    studies = [dict(zip(cols, row)) for row in study_rows]

    # Loci count
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM loci WHERE study_id IN ({ph})", params)
        n_loci = cur.fetchone()[0]
        cur.close()
    else:
        n_loci = conn.execute(
            f"SELECT COUNT(*) FROM loci WHERE study_id IN ({ph})", params
        ).fetchone()[0]

    # Evidence categories used
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT evidence_category FROM scored_evidence "
            f"WHERE study_id IN ({ph}) AND evidence_category IS NOT NULL",
            params,
        )
        categories = sorted(r[0] for r in cur.fetchall())
        cur.close()
    else:
        categories = sorted(
            r[0] for r in conn.execute(
                f"SELECT DISTINCT evidence_category FROM scored_evidence "
                f"WHERE study_id IN ({ph}) AND evidence_category IS NOT NULL",
                params,
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

    # Build PEGASUS-standard metadata structure
    base = studies[0]
    dataset = {
        "study_name": base.get("study_name"),
        "trait_description": base.get("trait_description"),
        "trait_ontology_id": base.get("trait_ontology_id"),
        "traits": [s["trait"] for s in studies],
        "peg_source": base.get("doi"),
    }
    dataset = {k: v for k, v in dataset.items() if v is not None}

    gwas = {}
    if base.get("gwas_source"):
        gwas["gwas_source"] = base["gwas_source"]
    if base.get("sample_size"):
        gwas["sample_size"] = base["sample_size"]
    if base.get("ancestry"):
        gwas["ancestry"] = base["ancestry"]
    if base.get("sex"):
        gwas["sex"] = base["sex"]

    genomic = {
        "genome_build": base.get("genome_build", "hg38"),
        "n_loci": n_loci,
    }

    source_metadata = [
        {"source_tag": r[0], "source_name": r[1], "evidence_category": r[2]}
        for r in ds_rows
    ]

    metadata: dict = {
        "dataset": dataset,
        "genomic": genomic,
        "evidence_categories": categories,
        "sources": source_metadata,
    }
    if gwas:
        metadata["gwas"] = gwas

    out_path = output_dir / "metadata.yaml"
    out_path.write_text(yaml.dump(metadata, default_flow_style=False, sort_keys=False))
    logger.info(f"Metadata → {out_path}")
    return out_path


def export_peg_list(conn: Any, study_ids: list[str], output_dir: Path) -> Path:
    """Export PEG list — rank-1 predicted effector gene per locus."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ph, params = _in_clause(study_ids, is_postgres(conn))

    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
            f"se.gene_symbol, se.integration_rank, se.is_predicted_effector "
            f"FROM scored_evidence se "
            f"JOIN loci l ON se.locus_id = l.locus_id "
            f"WHERE se.study_id IN ({ph}) AND se.integration_rank = 1 "
            f"ORDER BY l.chromosome, l.start_position",
            params,
        )
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
    else:
        result = conn.execute(
            f"SELECT DISTINCT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
            f"se.gene_symbol, se.integration_rank, se.is_predicted_effector "
            f"FROM scored_evidence se "
            f"JOIN loci l ON se.locus_id = l.locus_id "
            f"WHERE se.study_id IN ({ph}) AND se.integration_rank = 1 "
            f"ORDER BY l.chromosome, l.start_position",
            params,
        )
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()

    df = pd.DataFrame(rows, columns=cols)
    if len(df) > 0:
        df = df.drop_duplicates(subset=["locus_id"], keep="first")

    out_path = output_dir / "peg_list.tsv"
    df.to_csv(out_path, sep="\t", index=False)
    logger.info(f"PEG list: {len(df)} loci → {out_path}")
    return out_path


def export_all(conn: Any, study_name: str, output_dir: Path) -> dict[str, Path]:
    """Export all three PEGASUS deliverables.

    Accepts a study_name (e.g. 'shrine_2023') or a direct study_id.
    Returns dict of deliverable name → file path.
    """
    study_ids = _resolve_study_ids(conn, study_name)
    if not study_ids:
        raise ValueError(f"Study '{study_name}' not found")

    return {
        "evidence_matrix": export_evidence_matrix(conn, study_ids, output_dir),
        "metadata": export_metadata(conn, study_ids, output_dir),
        "peg_list": export_peg_list(conn, study_ids, output_dir),
    }
