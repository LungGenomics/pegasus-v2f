"""Data transformation and column cleaning."""

from __future__ import annotations

import re
import logging

import pandas as pd

from pegasus_v2f.report import Report

logger = logging.getLogger(__name__)


def clean_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names and types for database compatibility.

    Column name pipeline:
    1. Drop columns named like '...' (Excel artifacts: ...1, ...2)
    2. Replace periods with underscores
    3. Replace whitespace with underscores
    4. Replace non-alphanumeric/underscore chars with underscores
    5. Collapse consecutive underscores
    6. Strip leading/trailing underscores
    """
    # Drop unnamed Excel columns (... prefix)
    cols_to_drop = [c for c in df.columns if re.match(r"^\.\.\.|\bUnnamed", str(c))]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Clean column names
    new_cols = {}
    for col in df.columns:
        cleaned = str(col)
        cleaned = cleaned.replace(".", "_")
        cleaned = re.sub(r"\s+", "_", cleaned)
        cleaned = re.sub(r"[^A-Za-z0-9_]", "_", cleaned)
        cleaned = re.sub(r"_{2,}", "_", cleaned)
        cleaned = cleaned.strip("_")
        new_cols[col] = cleaned

    df = df.rename(columns=new_cols)

    # Convert problematic types to strings
    for col in df.columns:
        if df[col].dtype == "bool":
            df[col] = df[col].astype(str)
        elif df[col].dtype == "object":
            # Ensure lists/dicts in cells are stringified
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

    return df


def apply_transformations(
    df: pd.DataFrame,
    transformations: list[dict],
    report: Report | None = None,
) -> pd.DataFrame:
    """Apply a sequence of transformations to a DataFrame.

    Supported types: rename, select, deduplicate, custom.
    """
    for t in transformations:
        rows_before = len(df)
        try:
            t_type = t["type"]
            if t_type == "rename":
                df = _transform_rename(df, t)
            elif t_type == "select":
                df = _transform_select(df, t)
            elif t_type == "deduplicate":
                df = _transform_deduplicate(df, t)
            elif t_type == "custom":
                df = _transform_custom(df, t)
            else:
                logger.warning(f"Unknown transformation type: {t_type}")
                if report:
                    report.warning("unknown_transform", f"unknown transformation type: {t_type}")
                continue
        except Exception as e:
            logger.warning(f"Transformation failed ({t}): {e}")
            if report:
                report.error("transform_failed", f"{t_type} transformation failed: {e}")
            continue

        if report:
            rows_after = len(df)
            if rows_after != rows_before:
                report.info(
                    "transform_rows",
                    f"{t_type}: {rows_before} -> {rows_after} rows",
                )

    return df


def _transform_rename(df: pd.DataFrame, t: dict) -> pd.DataFrame:
    """Rename columns. Skips columns that don't exist."""
    mapping = t.get("columns", {})
    existing = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=existing)


def _transform_select(df: pd.DataFrame, t: dict) -> pd.DataFrame:
    """Select specific columns. Supports list or 'Start:End' range syntax."""
    columns = t.get("columns", [])

    if isinstance(columns, str) and ":" in columns:
        # Range syntax: "ColA:ColB"
        start, end = columns.split(":", 1)
        col_list = list(df.columns)
        try:
            start_idx = col_list.index(start)
            end_idx = col_list.index(end)
            columns = col_list[start_idx : end_idx + 1]
        except ValueError:
            logger.warning(f"Range columns not found: {start}:{end}")
            return df

    # Filter to columns that exist
    existing = [c for c in columns if c in df.columns]
    return df[existing]


def _transform_deduplicate(df: pd.DataFrame, t: dict) -> pd.DataFrame:
    """Remove duplicate rows based on a column."""
    column = t.get("column")
    if column and column in df.columns:
        return df.drop_duplicates(subset=[column], keep="first")
    return df


def _transform_custom(df: pd.DataFrame, t: dict) -> pd.DataFrame:
    """Apply a named custom transformation."""
    func_name = t.get("custom_function")

    if func_name == "parse_evidence":
        return parse_evidence(df)
    elif func_name == "apply_f_trait":
        return apply_f_trait(df)
    else:
        logger.warning(f"Unknown custom function: {func_name}")
        return df


# --- Custom transformation functions ---


def parse_evidence(df: pd.DataFrame) -> pd.DataFrame:
    """Explode evidence column with 'gene(term)' pairs into separate rows.

    Input: column 'evidence' containing strings like "GENE1(trait1), GENE2(trait2)"
    Output: original columns + 'gene' and 'term', one row per pair.
    """
    if "evidence" not in df.columns:
        logger.warning("parse_evidence: no 'evidence' column found")
        return df

    rows = []
    for idx, row in df.iterrows():
        ev = row.get("evidence")
        if pd.isna(ev) or not ev:
            rows.append({"_idx": idx, "gene": None, "term": None})
            continue

        pairs = str(ev).split(", ")
        for pair in pairs:
            gene = re.sub(r"\(.*", "", pair).strip()
            term = re.sub(r".*\(|\)", "", pair).strip()
            rows.append({"_idx": idx, "gene": gene or None, "term": term or None})

    expanded = pd.DataFrame(rows)
    original = df.drop(columns=["evidence"]).reset_index(drop=True)
    original["_idx"] = original.index

    merged = original.merge(expanded, on="_idx", how="right")
    return merged.drop(columns=["_idx"])


def apply_f_trait(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse trait evidence by gene, keeping best GWAS hit (lowest p-value).

    For each gene: collect all unique traits, keep the row with lowest minP.
    """
    if "gene" not in df.columns:
        return df

    # Collect all unique traits per gene
    trait_cols = [c for c in ["trait", "other_traits"] if c in df.columns]
    gene_traits = {}
    for _, row in df.iterrows():
        gene = row["gene"]
        if pd.isna(gene):
            continue
        traits = set()
        for col in trait_cols:
            val = row.get(col)
            if pd.notna(val):
                for t in str(val).split(","):
                    t = t.strip()
                    if t:
                        traits.add(t)
        gene_traits.setdefault(gene, set()).update(traits)

    trait_df = pd.DataFrame([
        {"gene": g, "trait": ",".join(sorted(ts))}
        for g, ts in gene_traits.items()
    ])

    # Best hit per gene (lowest minP)
    hit_cols = [c for c in ["gene", "minP", "rsid", "chr", "pos"] if c in df.columns]
    if "minP" in df.columns:
        hits = df[hit_cols].sort_values("minP").drop_duplicates(subset=["gene"], keep="first")
    else:
        hits = df[["gene"]].drop_duplicates()

    return trait_df.merge(hits, on="gene", how="left")
