"""Integration wizard — map raw tables to PEGASUS evidence categories."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

from pegasus_v2f.db import is_postgres
from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES

logger = logging.getLogger(__name__)

# Common column name patterns → suggested PEGASUS field mappings
_COLUMN_SUGGESTIONS: dict[str, list[str]] = {
    "gene": ["gene", "gene_symbol", "gene_name", "symbol", "hgnc"],
    "pvalue": ["pvalue", "p_value", "p.value", "minp", "pval", "p"],
    "chromosome": ["chr", "chromosome", "chrom"],
    "position": ["pos", "position", "bp", "start"],
    "rsid": ["rsid", "rs", "snp", "variant_id"],
    "effect_size": ["beta", "effect_size", "effect", "log_or", "or"],
    "score": ["score", "pip", "pph4", "h4", "posterior_prob"],
    "tissue": ["tissue", "cell_type", "tissue_name"],
}

# Category suggestions based on source name patterns
_NAME_CATEGORY_HINTS: dict[str, str] = {
    "coloc": "COLOC",
    "eqtl": "QTL",
    "sqtl": "QTL",
    "pqtl": "QTL",
    "qtl": "QTL",
    "gwas": "GWAS",
    "deg": "EXP",
    "expression": "EXP",
    "secretome": "KNOW",
    "drug": "DRUG",
    "omim": "RARE",
    "clinvar": "RARE",
}


def detect_columns(conn: Any, table_name: str) -> list[dict]:
    """Read column names, types, and sample values from a raw table.

    Returns list of dicts with keys: name, type, sample_values.
    """
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f'SELECT * FROM "{table_name}" LIMIT 5'
        )
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
    else:
        result = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 5')
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()

    columns = []
    for i, col in enumerate(cols):
        samples = [str(row[i]) for row in rows if row[i] is not None][:3]
        # Infer type from samples
        col_type = "text"
        if samples:
            try:
                [float(s) for s in samples]
                col_type = "numeric"
            except ValueError:
                pass
        columns.append({
            "name": col,
            "type": col_type,
            "sample_values": samples,
        })

    return columns


def suggest_mappings(
    columns: list[dict],
    source_name: str = "",
) -> dict:
    """Suggest PEGASUS field mappings based on column names.

    Returns dict with:
        - fields: dict of pegasus_field → column_name suggestions
        - category: suggested evidence category (or None)
        - centric: suggested centric type ('gene' or 'variant')
    """
    col_names = [c["name"] for c in columns]
    col_names_lower = [c.lower() for c in col_names]

    fields: dict[str, str] = {}
    for pegasus_field, patterns in _COLUMN_SUGGESTIONS.items():
        for pattern in patterns:
            for col, col_lower in zip(col_names, col_names_lower):
                if col_lower == pattern or col_lower.endswith(f"_{pattern}"):
                    fields[pegasus_field] = col
                    break
            if pegasus_field in fields:
                break

    # Guess category from source name
    category = None
    name_lower = source_name.lower()
    for hint, cat in _NAME_CATEGORY_HINTS.items():
        if hint in name_lower:
            category = cat
            break

    # Guess centric: if we found chromosome/position → variant, else gene
    has_position = "chromosome" in fields and "position" in fields
    centric = "variant" if has_position else "gene"

    return {
        "fields": fields,
        "category": category,
        "centric": centric,
    }


def validate_mapping(mapping: dict) -> list[str]:
    """Validate a proposed evidence mapping.

    Args:
        mapping: dict with keys: category, centric, source_tag, fields

    Returns:
        List of error strings (empty = valid).
    """
    errors = []

    category = mapping.get("category")
    if not category:
        errors.append("Evidence category is required")
    elif category not in EVIDENCE_CATEGORIES:
        errors.append(
            f"Unknown category '{category}'. "
            f"Valid: {', '.join(sorted(EVIDENCE_CATEGORIES))}"
        )

    centric = mapping.get("centric")
    if centric not in ("gene", "variant"):
        errors.append(f"Centric must be 'gene' or 'variant', got '{centric}'")

    if not mapping.get("source_tag"):
        errors.append("source_tag is required")

    fields = mapping.get("fields", {})
    if not fields.get("gene"):
        errors.append("Field mapping for 'gene' is required")

    if centric == "variant":
        if not fields.get("chromosome"):
            errors.append("Variant-centric mapping requires 'chromosome' field")
        if not fields.get("position"):
            errors.append("Variant-centric mapping requires 'position' field")

    return errors


def apply_integration(
    conn: Any,
    source_name: str,
    mapping: dict,
    config: dict,
    config_path: Path | None = None,
) -> dict:
    """Apply an evidence mapping to a raw table.

    1. Builds the evidence block from the mapping
    2. Writes it to v2f.yaml (in place) if config_path provided
    3. Loads the raw table through evidence routing
    4. Drops the raw table
    5. Re-runs scoring if loci exist

    Args:
        conn: Open database connection.
        source_name: Name of the raw table/source.
        mapping: dict with category, centric, source_tag, fields, evidence_type (optional).
        config: Full resolved config dict.
        config_path: Path to v2f.yaml (for in-place modification). None = skip file update.

    Returns:
        Summary dict with results.
    """
    from pegasus_v2f.evidence import route_evidence_source
    from pegasus_v2f.loaders import load_source
    from pegasus_v2f.transform import apply_transformations, clean_for_db

    # Build evidence block
    evidence_block = {
        "category": mapping["category"],
        "centric": mapping["centric"],
        "source_tag": mapping["source_tag"],
        "fields": mapping["fields"],
    }
    if mapping.get("evidence_type"):
        evidence_block["evidence_type"] = mapping["evidence_type"]

    # Find the source in config
    sources = config.get("data_sources", [])
    source = next((s for s in sources if s["name"] == source_name), None)
    if not source:
        raise ValueError(f"Source '{source_name}' not found in config")

    # Add evidence block to source
    source_with_evidence = {**source, "evidence": evidence_block}

    # Update v2f.yaml in place
    if config_path and config_path.exists():
        _update_yaml_evidence_block(config_path, source_name, evidence_block)

    # Load raw data from the existing table (it's already in the DB)
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{source_name}"')
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
    else:
        result = conn.execute(f'SELECT * FROM "{source_name}"')
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()

    import pandas as pd
    df = pd.DataFrame(rows, columns=cols)

    # Route through evidence loader
    load_result = route_evidence_source(conn, source_with_evidence, df, config)

    # Drop the raw table
    conn.execute(f'DROP TABLE IF EXISTS "{source_name}"')

    # Re-run scoring if loci exist
    n_scored = 0
    if config.get("pegasus"):
        loci_exist = conn.execute("SELECT COUNT(*) FROM loci").fetchone()[0] > 0
        if loci_exist:
            from pegasus_v2f.scoring import compute_locus_gene_scores
            n_scored = compute_locus_gene_scores(conn, config)

    summary = {
        "source": source_name,
        "evidence_block": evidence_block,
        "load_result": load_result,
        "raw_table_dropped": True,
        "scores_computed": n_scored,
    }
    logger.info(f"Integrated '{source_name}' as {mapping['category']} ({mapping['centric']})")
    return summary


def _update_yaml_evidence_block(
    config_path: Path,
    source_name: str,
    evidence_block: dict,
) -> None:
    """Insert an evidence block into v2f.yaml for the named source, in place.

    Uses string manipulation to preserve comments and formatting.
    """
    text = config_path.read_text()
    lines = text.split("\n")

    # Find the source block: look for "- name: <source_name>" under data_sources
    source_line_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == f"- name: {source_name}" or stripped == f'- name: "{source_name}"':
            source_line_idx = i
            break

    if source_line_idx is None:
        logger.warning(
            f"Could not find '- name: {source_name}' in {config_path}. "
            f"Skipping in-place YAML update."
        )
        return

    # Determine indentation of the source block
    source_line = lines[source_line_idx]
    base_indent = len(source_line) - len(source_line.lstrip())
    # Properties inside the source are indented 2 more than the "- " prefix
    prop_indent = base_indent + 2

    # Find the end of this source block (next "- name:" at same indent or end of list)
    insert_idx = source_line_idx + 1
    while insert_idx < len(lines):
        line = lines[insert_idx]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            insert_idx += 1
            continue
        line_indent = len(line) - len(line.lstrip())
        # Another list item at same level or less indent = end of this source
        if line_indent <= base_indent and stripped.startswith("- "):
            break
        if line_indent < base_indent:
            break
        # Check if evidence block already exists
        if stripped.startswith("evidence:"):
            logger.info(f"Source '{source_name}' already has evidence block — replacing")
            # Remove existing evidence block
            end_ev = insert_idx + 1
            while end_ev < len(lines):
                ev_line = lines[end_ev]
                ev_stripped = ev_line.strip()
                if not ev_stripped or ev_stripped.startswith("#"):
                    end_ev += 1
                    continue
                ev_indent = len(ev_line) - len(ev_line.lstrip())
                if ev_indent <= prop_indent:
                    break
                end_ev += 1
            del lines[insert_idx:end_ev]
            break
        insert_idx += 1

    # Build evidence YAML lines
    indent = " " * prop_indent
    sub_indent = " " * (prop_indent + 2)
    field_indent = " " * (prop_indent + 4)

    ev_lines = [f"{indent}evidence:"]
    ev_lines.append(f"{sub_indent}category: {evidence_block['category']}")
    ev_lines.append(f"{sub_indent}centric: {evidence_block['centric']}")
    ev_lines.append(f"{sub_indent}source_tag: {evidence_block['source_tag']}")
    if evidence_block.get("evidence_type"):
        ev_lines.append(f"{sub_indent}evidence_type: {evidence_block['evidence_type']}")
    if evidence_block.get("fields"):
        ev_lines.append(f"{sub_indent}fields:")
        for k, v in evidence_block["fields"].items():
            ev_lines.append(f"{field_indent}{k}: {v}")

    # Insert before the next source block
    for ev_line in reversed(ev_lines):
        lines.insert(insert_idx, ev_line)

    config_path.write_text("\n".join(lines))
    logger.info(f"Updated {config_path} with evidence block for '{source_name}'")
