"""Evidence loading — locus sources, evidence routing, and provenance."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from pegasus_v2f.db import is_postgres, write_table
from pegasus_v2f.evidence_config import resolve_evidence_mapping

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _cleanup_locus_source(conn: Any, source_tag: str) -> None:
    """Remove loci and evidence from a previous load of this source_tag.

    Deletes locus_gene_evidence rows, then orphaned loci (loci with no
    remaining evidence), then orphaned locus_gene_scores.
    """
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM locus_gene_evidence WHERE source_tag = %s",
            (source_tag,),
        )
        cur.execute(
            "DELETE FROM locus_gene_scores WHERE locus_id NOT IN "
            "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
        )
        cur.execute(
            "DELETE FROM loci WHERE locus_id NOT IN "
            "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "DELETE FROM locus_gene_evidence WHERE source_tag = ?",
            [source_tag],
        )
        conn.execute(
            "DELETE FROM locus_gene_scores WHERE locus_id NOT IN "
            "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
        )
        conn.execute(
            "DELETE FROM loci WHERE locus_id NOT IN "
            "(SELECT DISTINCT locus_id FROM locus_gene_evidence)"
        )
    logger.debug(f"Cleaned up previous evidence for source_tag '{source_tag}'")


def _cleanup_gene_evidence(conn: Any, source_tag: str) -> None:
    """Remove gene_evidence rows from a previous load of this source_tag."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM gene_evidence WHERE source_tag = %s", (source_tag,)
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "DELETE FROM gene_evidence WHERE source_tag = ?", [source_tag]
        )
    logger.debug(f"Cleaned up previous gene_evidence for source_tag '{source_tag}'")


def _resolve_study_config(source: dict, config: dict) -> dict:
    """Resolve which study config a source belongs to.

    Reads evidence.study from the source. If not set and exactly one study
    exists, auto-selects it. If not set and multiple studies exist, raises.
    """
    from pegasus_v2f.config import get_study_list

    studies = get_study_list(config)
    ev_study = source.get("evidence", {}).get("study")

    if ev_study:
        for s in studies:
            if s.get("id_prefix") == ev_study:
                return s
        raise ValueError(
            f"Source '{source.get('name', '?')}' references study '{ev_study}' "
            f"but no study with that id_prefix is configured"
        )

    if len(studies) == 1:
        return studies[0]

    if len(studies) == 0:
        raise ValueError("No studies configured in pegasus.study")

    raise ValueError(
        f"Source '{source.get('name', '?')}' has no evidence.study field "
        f"but {len(studies)} studies are configured. "
        f"Set evidence.study to one of: {', '.join(s['id_prefix'] for s in studies)}"
    )


def _create_studies(conn: Any, study_cfg: dict) -> list[str]:
    """Idempotent: create one study row per trait from a single study config dict.

    Returns list of study_ids created.
    """
    prefix = study_cfg["id_prefix"]
    traits = study_cfg["traits"]

    study_ids = []
    for trait in traits:
        study_id = f"{prefix}_{trait.lower()}"
        # Check if already exists
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM studies WHERE study_id = %s", (study_id,))
            exists = cur.fetchone() is not None
            cur.close()
        else:
            exists = conn.execute(
                "SELECT 1 FROM studies WHERE study_id = ?", [study_id]
            ).fetchone() is not None

        if not exists:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO studies (study_id, trait, gwas_source, ancestry) "
                    "VALUES (%s, %s, %s, %s)",
                    (study_id, trait, study_cfg.get("gwas_source"), study_cfg.get("ancestry")),
                )
                conn.commit()
                cur.close()
            else:
                conn.execute(
                    "INSERT INTO studies (study_id, trait, gwas_source, ancestry) "
                    "VALUES (?, ?, ?, ?)",
                    [study_id, trait, study_cfg.get("gwas_source"), study_cfg.get("ancestry")],
                )
        study_ids.append(study_id)

    return study_ids


def _match_to_loci(conn: Any, chromosome: str, position: int) -> list[dict]:
    """Find all loci that contain a given genomic position.

    Returns list of dicts with locus_id, study_id.
    """
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT locus_id, study_id FROM loci "
            "WHERE chromosome = %s AND start_position <= %s AND end_position >= %s",
            (str(chromosome), position, position),
        )
        rows = cur.fetchall()
        cur.close()
    else:
        rows = conn.execute(
            "SELECT locus_id, study_id FROM loci "
            "WHERE chromosome = ? AND start_position <= ? AND end_position >= ?",
            [str(chromosome), position, position],
        ).fetchall()

    return [{"locus_id": r[0], "study_id": r[1]} for r in rows]


def _upsert_data_source(
    conn: Any, source_tag: str, source_name: str, *,
    source_type: str | None = None,
    evidence_category: str | None = None,
    is_integrated: bool = True,
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
             is_integrated, now, record_count, url),
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
             is_integrated, now, record_count, url],
        )


def _get_locus_window_kb(config: dict) -> int:
    """Get locus window size from config (default 500kb)."""
    return config.get("pegasus", {}).get("locus_definition", {}).get("window_kb", 500)


# ---------------------------------------------------------------------------
# Locus definition loader (curated)
# ---------------------------------------------------------------------------

def load_locus_definition(
    conn: Any, source: dict, df: pd.DataFrame, config: dict
) -> dict:
    """Load curated locus definitions → studies + loci + locus_gene_evidence.

    Returns summary dict with counts.
    """
    evidence = source["evidence"]
    mapping = resolve_evidence_mapping(source, df)
    source_tag = evidence["source_tag"]

    # Clean up previous load of this source before re-inserting
    _cleanup_locus_source(conn, source_tag)

    window_kb = _get_locus_window_kb(config)
    merge_distance_kb = config.get("pegasus", {}).get(
        "locus_definition", {}
    ).get("merge_distance_kb", 250)

    # Resolve which study this source belongs to
    study_cfg = _resolve_study_config(source, config)
    study_ids = _create_studies(conn, study_cfg)
    traits = study_cfg["traits"]
    prefix = study_cfg["id_prefix"]

    # If evidence.trait is set, narrow to just that trait
    ev_trait = evidence.get("trait")
    if ev_trait:
        traits = [ev_trait]

    # Map trait → study_id
    trait_to_study = {t: f"{prefix}_{t.lower()}" for t in traits}

    # Extract columns via mapping
    gene_col = mapping["gene"]
    trait_col = mapping.get("trait")  # optional when evidence.trait is set
    chr_col = mapping["chromosome"]
    pos_col = mapping["position"]
    pval_col = mapping.get("pvalue")
    rsid_col = mapping.get("rsid")
    sentinel_col = mapping.get("sentinel")

    # Filter to rows with valid traits
    df = df.copy()
    valid_traits = set(traits)

    if ev_trait:
        # All rows belong to the declared trait — inject synthetic trait column
        trait_col = "__v2f_trait__"
        df[trait_col] = ev_trait
        df_valid = df
    else:
        df[trait_col] = df[trait_col].astype(str)
        df_valid = df[df[trait_col].isin(valid_traits)]

    if len(df_valid) == 0:
        col_info = f"Unique values in trait column: {df[trait_col].unique()[:10].tolist()}" if trait_col else ""
        logger.warning(
            f"No rows match declared traits {traits}. {col_info}"
        )
        return {"studies": len(study_ids), "loci": 0, "evidence_rows": 0}

    loci_created = 0
    evidence_rows = 0

    for trait in traits:
        study_id = trait_to_study[trait]
        trait_df = df_valid[df_valid[trait_col] == trait].copy()
        if len(trait_df) == 0:
            continue

        # Ensure position is numeric
        trait_df[pos_col] = pd.to_numeric(trait_df[pos_col], errors="coerce")
        trait_df = trait_df.dropna(subset=[pos_col])
        trait_df[pos_col] = trait_df[pos_col].astype(int)
        trait_df[chr_col] = trait_df[chr_col].astype(str)

        # Sort by p-value (best first) if available, else by position
        if pval_col and pval_col in trait_df.columns:
            trait_df[pval_col] = pd.to_numeric(trait_df[pval_col], errors="coerce")
            trait_df = trait_df.sort_values(pval_col)
        else:
            trait_df = trait_df.sort_values([chr_col, pos_col])

        # Group into loci by proximity (greedy clustering)
        loci = _cluster_into_loci(
            trait_df, chr_col, pos_col, gene_col,
            window_kb=window_kb, merge_distance_kb=merge_distance_kb,
            pval_col=pval_col, rsid_col=rsid_col,
            sentinel_col=sentinel_col,
        )

        for locus in loci:
            locus_id = f"{locus['lead_gene']}_{trait}"
            # Ensure unique locus_id
            locus_id = _ensure_unique_locus_id(conn, locus_id)

            _insert_locus(
                conn, locus_id=locus_id, study_id=study_id,
                locus_name=locus["lead_gene"],
                chromosome=locus["chromosome"],
                start_position=locus["start"],
                end_position=locus["end"],
                lead_variant_id=locus.get("lead_variant_id"),
                lead_rsid=locus.get("lead_rsid"),
                lead_pvalue=locus.get("lead_pvalue"),
                locus_source="curated",
            )
            loci_created += 1

            # Write evidence rows for each gene in this locus
            for gene in locus["genes"]:
                _insert_locus_gene_evidence(
                    conn, locus_id=locus_id, gene_symbol=gene,
                    evidence_category="GWAS", source_tag=source_tag,
                    pvalue=locus.get("lead_pvalue"),
                )
                evidence_rows += 1

    # Update n_loci counts on studies
    for study_id in study_ids:
        _update_study_locus_count(conn, study_id)

    # Provenance
    _upsert_data_source(
        conn, source_tag, source.get("name", source_tag),
        source_type=source.get("source_type"),
        evidence_category="GWAS",
        record_count=evidence_rows,
    )

    logger.info(
        f"Locus definition loaded: {loci_created} loci, {evidence_rows} evidence rows"
    )
    return {"studies": len(study_ids), "loci": loci_created, "evidence_rows": evidence_rows}


def _cluster_into_loci(
    df: pd.DataFrame,
    chr_col: str, pos_col: str, gene_col: str,
    window_kb: int, merge_distance_kb: int,
    pval_col: str | None = None,
    rsid_col: str | None = None,
    sentinel_col: str | None = None,
) -> list[dict]:
    """Cluster rows into loci by genomic proximity.

    Greedy: take the best row (by pvalue or first in order), assign all rows
    within window_kb on the same chromosome, repeat.
    """
    remaining = df.copy()
    loci = []

    while len(remaining) > 0:
        # Take the first row (best p-value or first by position)
        lead = remaining.iloc[0]
        chrom = str(lead[chr_col])
        pos = int(lead[pos_col])
        window = window_kb * 1000

        # Find all rows on same chromosome within window
        same_chrom = remaining[remaining[chr_col].astype(str) == chrom]
        in_window = same_chrom[
            (same_chrom[pos_col] >= pos - window)
            & (same_chrom[pos_col] <= pos + window)
        ]

        # Collect genes
        genes = in_window[gene_col].dropna().unique().tolist()
        genes = [str(g) for g in genes if g and str(g) != "nan"]

        # Locus boundaries
        positions = in_window[pos_col].tolist()
        start = min(positions) - window
        end = max(positions) + window

        # Extract lead variant ID from sentinel column (e.g. "1_1337837_A_G" → "1:1337837:A:G")
        lead_variant_id = None
        if sentinel_col and sentinel_col in remaining.columns and pd.notna(lead.get(sentinel_col)):
            raw = str(lead[sentinel_col]).strip()
            if raw:
                lead_variant_id = raw.replace("_", ":")

        locus = {
            "chromosome": chrom,
            "start": max(0, start),
            "end": end,
            "lead_gene": genes[0] if genes else f"chr{chrom}_{pos}",
            "genes": genes,
            "lead_variant_id": lead_variant_id,
            "lead_pvalue": float(lead[pval_col]) if pval_col and pd.notna(lead.get(pval_col)) else None,
            "lead_rsid": str(lead[rsid_col]) if rsid_col and rsid_col in remaining.columns and pd.notna(lead.get(rsid_col)) else None,
        }
        loci.append(locus)

        # Remove assigned rows
        remaining = remaining.drop(in_window.index)

    return loci


def _ensure_unique_locus_id(conn: Any, locus_id: str) -> str:
    """Append a suffix if locus_id already exists."""
    original = locus_id
    suffix = 1
    while True:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM loci WHERE locus_id = %s", (locus_id,))
            exists = cur.fetchone() is not None
            cur.close()
        else:
            exists = conn.execute(
                "SELECT 1 FROM loci WHERE locus_id = ?", [locus_id]
            ).fetchone() is not None
        if not exists:
            return locus_id
        suffix += 1
        locus_id = f"{original}_{suffix}"


def _insert_locus(
    conn: Any, *, locus_id: str, study_id: str, locus_name: str | None,
    chromosome: str, start_position: int, end_position: int,
    lead_variant_id: str | None = None, lead_rsid: str | None = None,
    lead_pvalue: float | None = None, locus_source: str = "curated",
) -> None:
    """Insert a single locus row."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO loci (locus_id, study_id, locus_name, chromosome, "
            "start_position, end_position, lead_variant_id, lead_rsid, lead_pvalue, locus_source) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (locus_id, study_id, locus_name, str(chromosome),
             int(start_position), int(end_position),
             lead_variant_id, lead_rsid, lead_pvalue, locus_source),
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "INSERT INTO loci (locus_id, study_id, locus_name, chromosome, "
            "start_position, end_position, lead_variant_id, lead_rsid, lead_pvalue, locus_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [locus_id, study_id, locus_name, str(chromosome),
             int(start_position), int(end_position),
             lead_variant_id, lead_rsid, lead_pvalue, locus_source],
        )


def _insert_locus_gene_evidence(
    conn: Any, *, locus_id: str, gene_symbol: str,
    evidence_category: str, source_tag: str,
    evidence_stream: str = "", pvalue: float | None = None,
    effect_size: float | None = None, score: float | None = None,
    tissue: str | None = None, cell_type: str | None = None,
    is_supporting: bool | None = None,
) -> None:
    """Insert a single locus_gene_evidence row (ignoring duplicates)."""
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO locus_gene_evidence "
                "(locus_id, gene_symbol, evidence_category, evidence_stream, source_tag, "
                "pvalue, effect_size, score, tissue, cell_type, is_supporting) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT DO NOTHING",
                (locus_id, gene_symbol, evidence_category, evidence_stream, source_tag,
                 pvalue, effect_size, score, tissue, cell_type, is_supporting),
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                "INSERT OR IGNORE INTO locus_gene_evidence "
                "(locus_id, gene_symbol, evidence_category, evidence_stream, source_tag, "
                "pvalue, effect_size, score, tissue, cell_type, is_supporting) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [locus_id, gene_symbol, evidence_category, evidence_stream, source_tag,
                 pvalue, effect_size, score, tissue, cell_type, is_supporting],
            )
    except Exception as e:
        logger.warning(f"Failed to insert evidence for {gene_symbol}@{locus_id}: {e}")


def _update_study_locus_count(conn: Any, study_id: str) -> None:
    """Update n_loci on a study row."""
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            "UPDATE studies SET n_loci = (SELECT COUNT(*) FROM loci WHERE study_id = %s) "
            "WHERE study_id = %s",
            (study_id, study_id),
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "UPDATE studies SET n_loci = (SELECT COUNT(*) FROM loci WHERE study_id = ?) "
            "WHERE study_id = ?",
            [study_id, study_id],
        )


# ---------------------------------------------------------------------------
# GWAS sumstats loader (auto-clumping)
# ---------------------------------------------------------------------------

def load_gwas_sumstats(
    conn: Any, source: dict, df: pd.DataFrame, config: dict
) -> dict:
    """Load GWAS summary statistics → variants + auto-clumped loci + evidence.

    Auto-clumps significant hits into loci. When curated loci exist,
    significant variants overlapping curated loci are assigned there;
    novel regions become new auto-clumped loci.

    Returns summary dict.
    """
    evidence = source["evidence"]
    mapping = resolve_evidence_mapping(source, df)
    source_tag = evidence["source_tag"]

    # Clean up previous load of this source before re-inserting
    _cleanup_locus_source(conn, source_tag)

    pvalue_threshold = float(evidence.get("pvalue_threshold", 5e-8))
    clump_distance_kb = int(evidence.get("clump_distance_kb", 500))
    window_kb = _get_locus_window_kb(config)

    # Resolve which study this source belongs to
    study_cfg = _resolve_study_config(source, config)
    study_ids = _create_studies(conn, study_cfg)
    prefix = study_cfg["id_prefix"]

    # gwas_sumstats uses evidence.trait to determine which trait
    ev_trait = evidence.get("trait")
    if ev_trait:
        traits = [ev_trait]
    else:
        traits = study_cfg["traits"]

    # Extract columns
    chr_col = mapping["chromosome"]
    pos_col = mapping["position"]
    pval_col = mapping["pvalue"]
    rsid_col = mapping.get("rsid")
    ref_col = mapping.get("ref_allele")
    alt_col = mapping.get("alt_allele")
    effect_col = mapping.get("effect_size")

    df = df.copy()
    df[pos_col] = pd.to_numeric(df[pos_col], errors="coerce")
    df[pval_col] = pd.to_numeric(df[pval_col], errors="coerce")
    df = df.dropna(subset=[chr_col, pos_col, pval_col])
    df[pos_col] = df[pos_col].astype(int)
    df[chr_col] = df[chr_col].astype(str)

    # Populate variants table
    variants_inserted = _populate_variants(
        conn, df, chr_col, pos_col, rsid_col, ref_col, alt_col
    )

    # Filter to significant hits
    sig = df[df[pval_col] <= pvalue_threshold].copy()
    if len(sig) == 0:
        logger.info(f"No significant variants (p <= {pvalue_threshold})")
        _upsert_data_source(
            conn, source_tag, source.get("name", source_tag),
            source_type=source.get("source_type"),
            evidence_category="GWAS",
            record_count=0,
        )
        return {"variants": variants_inserted, "loci": 0, "evidence_rows": 0}

    sig = sig.sort_values(pval_col)

    # Auto-clump: assign each significant variant to existing loci or create new ones
    loci_created = 0
    evidence_rows = 0

    # For each trait, process significant variants
    for trait in traits:
        study_id = f"{prefix}_{trait.lower()}"
        assigned = set()  # track assigned variant indices

        for idx, row in sig.iterrows():
            if idx in assigned:
                continue

            chrom = str(row[chr_col])
            pos = int(row[pos_col])
            pval = float(row[pval_col])

            # Check if this variant falls in an existing locus
            matching_loci = _match_to_loci(conn, chrom, pos)
            matching_for_study = [m for m in matching_loci if m["study_id"] == study_id]

            if matching_for_study:
                # Assign to existing locus (curated takes priority)
                locus_id = matching_for_study[0]["locus_id"]
            else:
                # Create new auto-clumped locus
                rsid_val = str(row[rsid_col]) if rsid_col and rsid_col in df.columns and pd.notna(row.get(rsid_col)) else None
                locus_id = f"chr{chrom}_{pos}_{trait}"
                locus_id = _ensure_unique_locus_id(conn, locus_id)

                _insert_locus(
                    conn, locus_id=locus_id, study_id=study_id,
                    locus_name=f"chr{chrom}:{pos}",
                    chromosome=chrom,
                    start_position=max(0, pos - window_kb * 1000),
                    end_position=pos + window_kb * 1000,
                    lead_rsid=rsid_val,
                    lead_pvalue=pval,
                    locus_source="auto_clumped",
                )
                loci_created += 1

            # Find nearby significant variants within clump_distance and assign them too
            nearby = sig[
                (sig[chr_col].astype(str) == chrom)
                & (sig[pos_col] >= pos - clump_distance_kb * 1000)
                & (sig[pos_col] <= pos + clump_distance_kb * 1000)
                & (~sig.index.isin(assigned))
            ]
            assigned.update(nearby.index)

            # Write GWAS evidence (one row per locus, using lead variant info)
            _insert_locus_gene_evidence(
                conn, locus_id=locus_id,
                gene_symbol="",  # no gene info from sumstats alone
                evidence_category="GWAS",
                source_tag=source_tag,
                pvalue=pval,
                effect_size=float(row[effect_col]) if effect_col and effect_col in df.columns and pd.notna(row.get(effect_col)) else None,
            )
            evidence_rows += 1

    # Update loci counts
    for study_id in study_ids:
        _update_study_locus_count(conn, study_id)

    _upsert_data_source(
        conn, source_tag, source.get("name", source_tag),
        source_type=source.get("source_type"),
        evidence_category="GWAS",
        record_count=evidence_rows,
    )

    logger.info(
        f"Sumstats loaded: {variants_inserted} variants, "
        f"{loci_created} new loci, {evidence_rows} evidence rows"
    )
    return {
        "variants": variants_inserted,
        "loci": loci_created,
        "evidence_rows": evidence_rows,
    }


def _populate_variants(
    conn: Any, df: pd.DataFrame,
    chr_col: str, pos_col: str,
    rsid_col: str | None, ref_col: str | None, alt_col: str | None,
) -> int:
    """Bulk-insert variants from sumstats DataFrame. Returns count inserted."""
    rows = []
    for _, row in df.iterrows():
        chrom = str(row[chr_col])
        pos = int(row[pos_col])
        ref = str(row[ref_col]) if ref_col and ref_col in df.columns and pd.notna(row.get(ref_col)) else None
        alt = str(row[alt_col]) if alt_col and alt_col in df.columns and pd.notna(row.get(alt_col)) else None
        rsid = str(row[rsid_col]) if rsid_col and rsid_col in df.columns and pd.notna(row.get(rsid_col)) else None

        # variant_id: chr:pos:ref:alt if alleles known, else chr:pos
        if ref and alt:
            variant_id = f"{chrom}:{pos}:{ref}:{alt}"
        else:
            variant_id = f"{chrom}:{pos}"

        rows.append((variant_id, rsid, chrom, pos, ref, alt))

    if not rows:
        return 0

    # Bulk insert (ignore duplicates)
    inserted = 0
    for batch_start in range(0, len(rows), 1000):
        batch = rows[batch_start:batch_start + 1000]
        for r in batch:
            try:
                if is_postgres(conn):
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO variants (variant_id, rsid, chromosome, position, ref_allele, alt_allele) "
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        r,
                    )
                    conn.commit()
                    cur.close()
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO variants "
                        "(variant_id, rsid, chromosome, position, ref_allele, alt_allele) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        list(r),
                    )
                inserted += 1
            except Exception:
                pass  # duplicate or constraint violation

    return inserted


# ---------------------------------------------------------------------------
# Gene-level evidence loader
# ---------------------------------------------------------------------------

def load_gene_evidence(conn: Any, source: dict, df: pd.DataFrame) -> dict:
    """Load gene-level evidence → gene_evidence table.

    Returns summary dict.
    """
    evidence = source["evidence"]
    mapping = resolve_evidence_mapping(source, df)
    source_tag = evidence["source_tag"]

    # Clean up previous load of this source before re-inserting
    _cleanup_gene_evidence(conn, source_tag)

    category = evidence["category"]
    evidence_type = evidence.get("evidence_type", category.lower())

    gene_col = mapping["gene"]
    score_col = mapping.get("score")
    tissue_col = mapping.get("tissue")
    cell_type_col = mapping.get("cell_type")

    # Trait-specific evidence (empty string = applies to all traits)
    ev_trait = evidence.get("trait") or ""

    inserted = 0
    for _, row in df.iterrows():
        gene = str(row[gene_col])
        if not gene or gene == "nan":
            continue

        score = float(row[score_col]) if score_col and score_col in df.columns and pd.notna(row.get(score_col)) else None
        tissue = str(row[tissue_col]) if tissue_col and tissue_col in df.columns and pd.notna(row.get(tissue_col)) else None
        cell_type = str(row[cell_type_col]) if cell_type_col and cell_type_col in df.columns and pd.notna(row.get(cell_type_col)) else None

        try:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO gene_evidence "
                    "(gene_symbol, evidence_category, evidence_type, source_tag, "
                    "trait, score, tissue, cell_type) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (gene, category, evidence_type, source_tag, ev_trait, score, tissue, cell_type),
                )
                conn.commit()
                cur.close()
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO gene_evidence "
                    "(gene_symbol, evidence_category, evidence_type, source_tag, "
                    "trait, score, tissue, cell_type) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [gene, category, evidence_type, source_tag, ev_trait, score, tissue, cell_type],
                )
            inserted += 1
        except Exception as e:
            logger.warning(f"Failed to insert gene evidence for {gene}: {e}")

    _upsert_data_source(
        conn, source_tag, source.get("name", source_tag),
        source_type=source.get("source_type"),
        evidence_category=category,
        record_count=inserted,
    )

    logger.info(f"Gene evidence loaded: {inserted} rows ({category})")
    return {"evidence_rows": inserted}


# ---------------------------------------------------------------------------
# Variant-level evidence loader
# ---------------------------------------------------------------------------

def load_variant_evidence(conn: Any, source: dict, df: pd.DataFrame) -> dict:
    """Load variant-level evidence → locus_gene_evidence (matched to loci by position).

    Returns summary dict.
    """
    evidence = source["evidence"]
    mapping = resolve_evidence_mapping(source, df)
    source_tag = evidence["source_tag"]
    category = evidence["category"]
    evidence_stream = evidence.get("evidence_stream", "")

    gene_col = mapping["gene"]
    chr_col = mapping.get("chromosome")
    pos_col = mapping.get("position")
    pval_col = mapping.get("pvalue")
    score_col = mapping.get("score")
    tissue_col = mapping.get("tissue")

    inserted = 0
    unmatched = 0

    for _, row in df.iterrows():
        gene = str(row[gene_col])
        if not gene or gene == "nan":
            continue

        # Try to match to loci
        loci_matched = []
        if chr_col and pos_col and chr_col in df.columns and pos_col in df.columns:
            chrom = str(row[chr_col])
            pos_val = row[pos_col]
            if pd.notna(pos_val):
                loci_matched = _match_to_loci(conn, chrom, int(pos_val))

        if not loci_matched:
            # Try matching by gene name across all loci
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(
                    "SELECT DISTINCT locus_id FROM locus_gene_evidence WHERE gene_symbol = %s",
                    (gene,),
                )
                rows = cur.fetchall()
                cur.close()
            else:
                rows = conn.execute(
                    "SELECT DISTINCT locus_id FROM locus_gene_evidence WHERE gene_symbol = ?",
                    [gene],
                ).fetchall()
            loci_matched = [{"locus_id": r[0]} for r in rows]

        if not loci_matched:
            unmatched += 1
            continue

        pvalue = float(row[pval_col]) if pval_col and pval_col in df.columns and pd.notna(row.get(pval_col)) else None
        score = float(row[score_col]) if score_col and score_col in df.columns and pd.notna(row.get(score_col)) else None
        tissue = str(row[tissue_col]) if tissue_col and tissue_col in df.columns and pd.notna(row.get(tissue_col)) else None

        for locus in loci_matched:
            _insert_locus_gene_evidence(
                conn, locus_id=locus["locus_id"], gene_symbol=gene,
                evidence_category=category, evidence_stream=evidence_stream,
                source_tag=source_tag, pvalue=pvalue, score=score, tissue=tissue,
            )
            inserted += 1

    if unmatched:
        logger.warning(f"  {unmatched} rows could not be matched to any locus")

    _upsert_data_source(
        conn, source_tag, source.get("name", source_tag),
        source_type=source.get("source_type"),
        evidence_category=category,
        record_count=inserted,
    )

    logger.info(f"Variant evidence loaded: {inserted} rows ({category}), {unmatched} unmatched")
    return {"evidence_rows": inserted, "unmatched": unmatched}


# ---------------------------------------------------------------------------
# Evidence routing (called from pipeline)
# ---------------------------------------------------------------------------

def route_evidence_source(
    conn: Any, source: dict, df: pd.DataFrame, config: dict
) -> dict | None:
    """Route a source through the appropriate evidence loader.

    Returns summary dict if processed, None if not an evidence source.
    """
    evidence = source.get("evidence")
    if not evidence:
        return None  # Raw source — not our concern

    role = evidence.get("role")
    centric = evidence.get("centric")

    if role == "locus_definition":
        return load_locus_definition(conn, source, df, config)
    elif role == "gwas_sumstats":
        return load_gwas_sumstats(conn, source, df, config)
    elif centric == "gene":
        return load_gene_evidence(conn, source, df)
    elif centric == "variant":
        return load_variant_evidence(conn, source, df)
    else:
        logger.warning(f"Unknown evidence routing for source '{source.get('name')}'")
        return None
