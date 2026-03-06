"""Study management — add studies with loci from sentinel variant files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pegasus_v2f.db import is_postgres, write_table

logger = logging.getLogger(__name__)

# Default locus window and merge parameters
DEFAULT_WINDOW_KB = 500
DEFAULT_MERGE_DISTANCE_KB = 250


def add_study(
    conn: Any,
    study_name: str,
    traits: list[str],
    loci_file: Path | None = None,
    loci_df: pd.DataFrame | None = None,
    *,
    gwas_source: str | None = None,
    ancestry: str | None = None,
    sex: str | None = None,
    sample_size: int | None = None,
    doi: str | None = None,
    year: int | None = None,
    genome_build: str | None = None,
    gene_column: str | None = None,
    sentinel_column: str | None = None,
    pvalue_column: str | None = None,
    rsid_column: str | None = None,
    loci_source: str | None = None,
    loci_sheet: str | None = None,
    loci_skip: int | None = None,
    window_kb: int = DEFAULT_WINDOW_KB,
    merge_distance_kb: int = DEFAULT_MERGE_DISTANCE_KB,
    cache_dir: Path | None = None,
    config_path: Path | None = None,
) -> dict:
    """Add a study with loci from a sentinel variant file.

    Creates study-trait rows in the ``studies`` table, reads sentinel variants,
    clusters them into loci using ``window_kb`` and ``merge_distance_kb``,
    inserts loci, and stores the raw sentinels as ``loci_<study_name>``.

    Either *loci_file* or *loci_df* must be provided.

    Args:
        conn: Open database connection.
        study_name: Unique study name (used as ``study_name`` column and for IDs).
        traits: List of trait names for this study.
        loci_file: Path to sentinel variant TSV/CSV file.
        loci_df: Pre-loaded DataFrame of sentinel variants (alternative to file).
        gwas_source: GWAS source identifier (e.g. PMID).
        ancestry: Population ancestry.
        sex: Sex restriction (e.g. "male", "female", "both").
        sample_size: GWAS sample size.
        doi: Publication DOI.
        year: Publication year.
        genome_build: Genome build (e.g. "hg38", "GRCh38").
        window_kb: Locus window half-size in kb (default 500).
        merge_distance_kb: Merge overlapping loci within this distance in kb.
        cache_dir: Directory for cached reference data (e.g. cytoband file).
            Falls back to tempdir if not provided.
        config_path: Path to v2f.yaml. If provided, study config is written
            to both v2f.yaml and ``_pegasus_meta`` (keeps them in sync).

    Returns:
        Summary dict with study_ids, n_loci, n_sentinels.
    """
    if loci_file is None and loci_df is None and loci_source is None:
        raise ValueError("Either loci_file, loci_df, or loci_source must be provided")

    if not traits:
        raise ValueError("At least one trait is required")

    # Check for duplicate traits
    if len(traits) != len(set(traits)):
        raise ValueError(f"Duplicate traits: {[t for t in traits if traits.count(t) > 1]}")

    # Load sentinel data
    if loci_df is None:
        if loci_source is not None and loci_source.startswith("http"):
            from pegasus_v2f.loaders import load_googlesheets
            source_spec = {"url": loci_source, "source_type": "googlesheets"}
            if loci_sheet:
                source_spec["sheet"] = loci_sheet
            if loci_skip:
                source_spec["skip_rows"] = loci_skip
            loci_df = load_googlesheets(source_spec)
        elif loci_source is not None:
            loci_file = Path(loci_source)
        # Fall through to file loading
        if loci_df is None and loci_file is not None:
            if loci_file.suffix.lower() in (".xlsx", ".xls"):
                kwargs = {"engine": "calamine"}
                if loci_sheet:
                    kwargs["sheet_name"] = loci_sheet
                if loci_skip:
                    kwargs["skiprows"] = loci_skip
                loci_df = pd.read_excel(loci_file, **kwargs)
            else:
                loci_df = _read_sentinel_file(loci_file)

    if loci_df is None:
        raise ValueError("Could not load sentinel data from any source")

    # Normalize column names
    loci_df.columns = [c.strip().lower() for c in loci_df.columns]

    # Validate required columns
    if "chromosome" not in loci_df.columns and "chr" not in loci_df.columns:
        raise ValueError("Sentinel file must have 'chromosome' or 'chr' column")
    if "position" not in loci_df.columns and "pos" not in loci_df.columns:
        raise ValueError("Sentinel file must have 'position' or 'pos' column")

    # Normalize chr/pos column names
    if "chr" in loci_df.columns and "chromosome" not in loci_df.columns:
        loci_df = loci_df.rename(columns={"chr": "chromosome"})
    if "pos" in loci_df.columns and "position" not in loci_df.columns:
        loci_df = loci_df.rename(columns={"pos": "position"})

    # Ensure chromosome is string, position is int
    loci_df["chromosome"] = loci_df["chromosome"].astype(str)
    loci_df["position"] = pd.to_numeric(loci_df["position"], errors="coerce").astype("Int64")
    loci_df = loci_df.dropna(subset=["chromosome", "position"])

    # Store raw sentinels as loci_<study_name>
    raw_table = f"loci_{study_name}"
    write_table(conn, raw_table, loci_df)
    logger.info(f"Stored {len(loci_df)} sentinel variants as {raw_table}")

    # Determine if file has a trait column
    has_trait_col = "trait" in loci_df.columns
    study_ids = []
    total_loci = 0

    for trait in traits:
        study_id = f"{study_name}_{trait.lower().replace('/', '_')}"

        # Create study row
        _insert_study(
            conn,
            study_id=study_id,
            study_name=study_name,
            trait=trait,
            gwas_source=gwas_source,
            ancestry=ancestry,
            sex=sex,
            sample_size=sample_size,
            doi=doi,
            year=year,
            genome_build=genome_build,
        )
        study_ids.append(study_id)

        # Get sentinels for this trait
        if has_trait_col:
            trait_sentinels = loci_df[loci_df["trait"] == trait].copy()
        else:
            trait_sentinels = loci_df.copy()

        if trait_sentinels.empty:
            logger.warning(f"No sentinels for trait '{trait}' — skipping loci creation")
            continue

        # Cluster sentinels into loci
        loci = _cluster_sentinels(
            trait_sentinels,
            window_kb=window_kb,
            merge_distance_kb=merge_distance_kb,
        )

        # Insert loci
        n_loci = _insert_loci(conn, loci, study_id, trait=trait,
                              cache_dir=cache_dir, gene_column=gene_column,
                              sentinel_column=sentinel_column,
                              pvalue_column=pvalue_column,
                              rsid_column=rsid_column)
        total_loci += n_loci

        # Update n_loci on study
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "UPDATE studies SET n_loci = %s WHERE study_id = %s",
                (n_loci, study_id),
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                "UPDATE studies SET n_loci = ? WHERE study_id = ?",
                [n_loci, study_id],
            )

        logger.info(f"Created {n_loci} loci for {study_id}")

    # Write study config to v2f.yaml if config_path provided
    if config_path is not None:
        _sync_study_to_yaml(
            config_path, study_name, traits,
            gwas_source=gwas_source, ancestry=ancestry, sex=sex,
            sample_size=sample_size, doi=doi, year=year,
            genome_build=genome_build, gene_column=gene_column,
            sentinel_column=sentinel_column, pvalue_column=pvalue_column,
            rsid_column=rsid_column,
            loci_source=loci_source, loci_sheet=loci_sheet, loci_skip=loci_skip,
            window_kb=window_kb, merge_distance_kb=merge_distance_kb,
        )

    # Propagate study config to _pegasus_meta
    _sync_study_to_meta(conn, study_name, traits, gwas_source=gwas_source,
                        ancestry=ancestry, sex=sex, sample_size=sample_size,
                        doi=doi, year=year, genome_build=genome_build)

    return {
        "study_name": study_name,
        "study_ids": study_ids,
        "n_loci": total_loci,
        "n_sentinels": len(loci_df),
    }


def remove_study(conn: Any, study_name: str) -> int:
    """Remove a study and all its loci/scored_evidence from the database.

    Deletes rows from scored_evidence, loci, and studies where the study_name
    matches. Also drops the raw sentinels table and removes from _pegasus_meta.

    Returns number of study rows deleted.
    """
    import yaml
    from pegasus_v2f.db_meta import read_meta, write_meta

    # Find all study_ids for this study_name
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM studies WHERE study_name = %s", (study_name,))
        study_ids = [r[0] for r in cur.fetchall()]
        cur.close()
    else:
        rows = conn.execute(
            "SELECT study_id FROM studies WHERE study_name = ?", [study_name]
        ).fetchall()
        study_ids = [r[0] for r in rows]

    if not study_ids:
        raise ValueError(f"Study '{study_name}' not found in database")

    for sid in study_ids:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute("DELETE FROM scored_evidence WHERE study_id = %s", (sid,))
            cur.execute("DELETE FROM loci WHERE study_id = %s", (sid,))
            cur.execute("DELETE FROM studies WHERE study_id = %s", (sid,))
            cur.close()
        else:
            conn.execute("DELETE FROM scored_evidence WHERE study_id = ?", [sid])
            conn.execute("DELETE FROM loci WHERE study_id = ?", [sid])
            conn.execute("DELETE FROM studies WHERE study_id = ?", [sid])

    if is_postgres(conn):
        conn.commit()

    # Drop raw sentinels table
    raw_table = f"loci_{study_name}"
    try:
        conn.execute(f"DROP TABLE IF EXISTS \"{raw_table}\"")
        if is_postgres(conn):
            conn.commit()
    except Exception:
        pass

    # Remove from _pegasus_meta config
    config_yaml = read_meta(conn, "config")
    if config_yaml:
        meta_config = yaml.safe_load(config_yaml) or {}
        studies = meta_config.get("pegasus", {}).get("study", [])
        if isinstance(studies, dict):
            studies = [studies]
        meta_config.setdefault("pegasus", {})["study"] = [
            s for s in studies if s.get("id_prefix") != study_name
        ]
        write_meta(conn, "config", yaml.dump(meta_config, default_flow_style=False, sort_keys=False))

    logger.info(f"Removed study '{study_name}': {len(study_ids)} study rows deleted")
    return len(study_ids)


def preview_study(
    conn: Any,
    study_name: str,
) -> list[dict]:
    """Preview scoring for a study — spatial join without materializing.

    For each locus in the study, finds candidate genes and matching evidence.

    Returns:
        List of dicts, one per locus, with candidate gene count and evidence
        counts by category.
    """
    from pegasus_v2f.scoring import (
        _get_candidate_genes_by_geometry,
        _get_gene_level_evidence,
        _get_variant_evidence_in_window,
        _get_loci,
    )

    loci = _get_loci(conn, study_name)
    if not loci:
        return []

    # Warn if genes table is empty — candidate gene counts will be zero
    try:
        gene_count = conn.execute("SELECT COUNT(*) FROM genes").fetchone()[0]
        if gene_count == 0:
            logger.warning(
                "genes table is empty — candidate gene counts will be 0. "
                "Run `v2f rescore` to fetch gene annotations first."
            )
    except Exception:
        pass

    results = []
    for locus in loci:
        locus_chr = locus["chromosome"]
        locus_start = locus["start_position"]
        locus_end = locus["end_position"]

        # Candidate genes by geometry
        candidates = _get_candidate_genes_by_geometry(
            conn, locus_chr, locus_start, locus_end
        )

        # Variant evidence in window
        variant_ev = _get_variant_evidence_in_window(
            conn, locus_chr, locus_start, locus_end
        )

        # Gene-level evidence for all relevant genes
        all_genes = set(candidates)
        for ev in variant_ev:
            all_genes.add(ev["gene_symbol"])
        gene_ev = _get_gene_level_evidence(conn, all_genes)

        # Count evidence by category
        category_counts: dict[str, int] = {}
        for ev in variant_ev + gene_ev:
            cat = ev.get("evidence_category", "?")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        results.append({
            "locus_id": locus["locus_id"],
            "chromosome": locus_chr,
            "start_position": locus_start,
            "end_position": locus_end,
            "n_candidate_genes": len(set(candidates) | all_genes),
            "n_evidence_rows": len(variant_ev) + len(gene_ev),
            "evidence_by_category": category_counts,
        })

    return results


def _read_sentinel_file(path: Path) -> pd.DataFrame:
    """Read a sentinel variant file (TSV or CSV)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Sentinel file not found: {path}")

    suffix = path.suffix.lower()
    if suffix in (".tsv", ".gz"):
        return pd.read_csv(path, sep="\t")
    elif suffix == ".csv":
        return pd.read_csv(path)
    else:
        # Try TSV first, fall back to CSV
        try:
            df = pd.read_csv(path, sep="\t")
            if len(df.columns) > 1:
                return df
        except Exception:
            pass
        return pd.read_csv(path)


def _insert_study(
    conn: Any,
    *,
    study_id: str,
    study_name: str,
    trait: str,
    gwas_source: str | None = None,
    ancestry: str | None = None,
    sex: str | None = None,
    sample_size: int | None = None,
    doi: str | None = None,
    year: int | None = None,
    genome_build: str | None = None,
) -> None:
    """Insert a single study row."""
    cols = "study_id, study_name, trait"
    vals = [study_id, study_name, trait]
    optional = {
        "gwas_source": gwas_source,
        "ancestry": ancestry,
        "sex": sex,
        "sample_size": sample_size,
        "doi": doi,
        "year": year,
        "genome_build": genome_build,
    }
    for col, val in optional.items():
        if val is not None:
            cols += f", {col}"
            vals.append(val)

    if is_postgres(conn):
        placeholders = ", ".join(["%s"] * len(vals))
        cur = conn.cursor()
        cur.execute(f"INSERT INTO studies ({cols}) VALUES ({placeholders})", vals)
        conn.commit()
        cur.close()
    else:
        placeholders = ", ".join(["?"] * len(vals))
        conn.execute(f"INSERT INTO studies ({cols}) VALUES ({placeholders})", vals)


def _cluster_sentinels(
    df: pd.DataFrame,
    window_kb: int = DEFAULT_WINDOW_KB,
    merge_distance_kb: int = DEFAULT_MERGE_DISTANCE_KB,
) -> list[dict]:
    """Cluster sentinel variants into loci.

    For each sentinel, create a window of ±window_kb around its position.
    Then merge overlapping windows (within merge_distance_kb).

    Returns list of locus dicts with chromosome, start, end, and sentinels list.
    """
    window = window_kb * 1000
    merge_dist = merge_distance_kb * 1000

    loci = []

    for chrom, group in df.groupby("chromosome"):
        group = group.sort_values("position")

        # Create initial windows
        windows = []
        for _, row in group.iterrows():
            pos = int(row["position"])
            windows.append({
                "chromosome": str(chrom),
                "start": max(0, pos - window),
                "end": pos + window,
                "sentinels": [row.to_dict()],
            })

        # Merge overlapping windows
        merged = _merge_windows(windows, merge_dist)
        loci.extend(merged)

    return loci


def _merge_windows(windows: list[dict], merge_distance: int) -> list[dict]:
    """Merge overlapping or nearby windows."""
    if not windows:
        return []

    windows.sort(key=lambda w: w["start"])
    merged = [windows[0]]

    for w in windows[1:]:
        prev = merged[-1]
        if w["start"] <= prev["end"] + merge_distance:
            # Merge
            prev["end"] = max(prev["end"], w["end"])
            prev["sentinels"].extend(w["sentinels"])
        else:
            merged.append(w)

    return merged


def _parse_variant_id(value: str) -> dict | None:
    """Parse a variant ID in chr_pos_ref_alt format.

    Accepts any non-alphanumeric separator (_, :, -, /).
    Returns dict with chromosome, position, ref_allele, alt_allele, or None on failure.
    """
    import re
    parts = re.split(r'[^a-zA-Z0-9]+', value.strip())
    if len(parts) < 4:
        return None
    try:
        return {
            "chromosome": parts[0],
            "position": int(parts[1]),
            "ref_allele": parts[2],
            "alt_allele": parts[3],
        }
    except (ValueError, IndexError):
        return None


def _collect_sentinel_values(sentinels: list[dict], column: str | None) -> str | None:
    """Collect unique non-empty values from a column across all sentinels, comma-separated."""
    if not column:
        return None
    col_lower = column.lower()
    seen = []
    for s in sentinels:
        v = s.get(col_lower)
        if v is not None:
            v = str(v).strip()
            if v and v not in seen:
                seen.append(v)
    return ", ".join(seen) if seen else None




def _insert_loci(
    conn: Any, loci: list[dict], study_id: str,
    trait: str | None = None,
    cache_dir: Path | None = None,
    gene_column: str | None = None,
    sentinel_column: str | None = None,
    pvalue_column: str | None = None,
    rsid_column: str | None = None,
) -> int:
    """Insert loci into the loci table. Returns count inserted."""
    count = 0
    for i, locus in enumerate(loci, 1):
        locus_id = f"{study_id}_L{i:04d}"

        sentinels = locus["sentinels"]

        # Cytoband-based locus naming (neutral — no gene bias)
        locus_name = _get_locus_name(
            locus["chromosome"], locus["start"], locus["end"], cache_dir
        )

        n_signals = len(sentinels)

        # Collect values across all sentinels (comma-separated for merged loci)
        nearest_gene = _collect_sentinel_values(sentinels, gene_column)
        lead_variant_id = _collect_sentinel_values(sentinels, sentinel_column)
        lead_rsid = _collect_sentinel_values(sentinels, rsid_column)
        lead_pvalue = _collect_sentinel_values(sentinels, pvalue_column)

        vals = [
            locus_id,
            study_id,
            trait,
            locus_name,
            str(locus["chromosome"]),
            locus["start"],
            locus["end"],
            lead_variant_id,
            lead_rsid,
            lead_pvalue,
            nearest_gene,
            "sentinel_clustering",
            n_signals,
        ]

        cols = (
            "locus_id, study_id, trait, locus_name, chromosome, start_position, "
            "end_position, lead_variant_id, lead_rsid, lead_pvalue, "
            "nearest_gene, locus_source, n_signals"
        )

        if is_postgres(conn):
            placeholders = ", ".join(["%s"] * len(vals))
            cur = conn.cursor()
            cur.execute(f"INSERT INTO loci ({cols}) VALUES ({placeholders})", vals)
            cur.close()
        else:
            placeholders = ", ".join(["?"] * len(vals))
            conn.execute(f"INSERT INTO loci ({cols}) VALUES ({placeholders})", vals)

        count += 1

    if is_postgres(conn):
        conn.commit()

    return count


def _get_locus_name(
    chromosome: str, start: int, end: int, cache_dir: Path | None,
) -> str:
    """Get a cytoband-based locus name for a genomic region.

    Falls back to chr:pos format if cytoband lookup fails or no cache_dir.
    """
    if cache_dir is not None:
        try:
            from pegasus_v2f.cytoband import get_cytoband_for_region
            return get_cytoband_for_region(chromosome, start, end, cache_dir)
        except Exception as e:
            logger.warning(f"Cytoband lookup failed: {e}")
    return f"chr{chromosome}:{start}-{end}"



def _sync_study_to_meta(
    conn: Any,
    study_name: str,
    traits: list[str],
    **kwargs: Any,
) -> None:
    """Sync study config into _pegasus_meta so the DB is self-describing."""
    import yaml
    from pegasus_v2f.db_meta import read_meta, write_meta

    config_yaml = read_meta(conn, "config")
    if config_yaml:
        config = yaml.safe_load(config_yaml) or {}
    else:
        config = {"version": 1}

    config.setdefault("pegasus", {})
    studies = config["pegasus"].get("study", [])
    if isinstance(studies, dict):
        studies = [studies]

    # Remove existing entry for this study_name if re-adding
    studies = [s for s in studies if s.get("id_prefix") != study_name]

    study_entry = {"id_prefix": study_name, "traits": traits}
    for key in ("gwas_source", "ancestry", "sex", "sample_size", "doi", "year", "genome_build"):
        if kwargs.get(key) is not None:
            study_entry[key] = kwargs[key]
    studies.append(study_entry)

    config["pegasus"]["study"] = studies
    write_meta(conn, "config", yaml.dump(config, default_flow_style=False, sort_keys=False))


def _sync_study_to_yaml(
    config_path: Path,
    study_name: str,
    traits: list[str],
    *,
    window_kb: int = DEFAULT_WINDOW_KB,
    merge_distance_kb: int = DEFAULT_MERGE_DISTANCE_KB,
    **kwargs: Any,
) -> None:
    """Write study config to v2f.yaml, keeping it in sync with _pegasus_meta."""
    from pegasus_v2f.config import add_study_to_yaml

    study_config = {"id_prefix": study_name, "traits": traits}
    for key in ("gwas_source", "ancestry", "sex", "sample_size", "doi", "year",
                 "genome_build", "loci_source", "loci_sheet", "loci_skip",
                 "gene_column", "sentinel_column", "pvalue_column", "rsid_column"):
        if kwargs.get(key) is not None:
            study_config[key] = kwargs[key]

    # Only set locus_definition if it's not already in the yaml
    locus_config = {"window_kb": window_kb, "merge_distance_kb": merge_distance_kb}

    try:
        add_study_to_yaml(config_path, study_config, locus_config)
    except ValueError:
        # Study already exists in yaml (e.g. CLI wrote it first) — not an error
        pass
