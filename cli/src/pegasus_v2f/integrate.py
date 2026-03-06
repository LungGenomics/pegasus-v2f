"""Integration wizard — map raw tables to PEGASUS evidence categories."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

from pegasus_v2f.db import is_postgres, raw_table_name
from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES, EVIDENCE_CATEGORY_PROFILES

logger = logging.getLogger(__name__)

# Common column name patterns → suggested PEGASUS field mappings
_COLUMN_SUGGESTIONS: dict[str, list[str]] = {
    "gene": ["gene", "gene_symbol", "gene_name", "symbol", "hgnc"],
    "trait": ["trait", "phenotype", "pheno"],
    "pvalue": ["pvalue", "p_value", "p.value", "minp", "pval", "p", "min_p"],
    "chromosome": ["chr", "chromosome", "chrom"],
    "position": ["pos", "position", "bp", "start"],
    "sentinel": ["sentinel", "lead_variant", "lead_snp", "index_variant", "index_snp"],
    "rsid": ["rsid", "rs", "snp", "variant_id"],
    "effect_size": ["beta", "effect_size", "effect", "log_or", "or"],
    "score": ["score", "pip", "pph4", "h4", "posterior_prob"],
    "tissue": ["tissue", "cell_type", "tissue_name"],
}

# Category suggestions derived from CategoryProfile.source_name_hints
_NAME_CATEGORY_HINTS: dict[str, str] = {}
for _abbrev, _profile in EVIDENCE_CATEGORY_PROFILES.items():
    for _hint in _profile.source_name_hints:
        if _hint not in _NAME_CATEGORY_HINTS:
            _NAME_CATEGORY_HINTS[_hint] = _abbrev


def detect_columns_from_df(df) -> list[dict]:
    """Build column info dicts from a DataFrame.

    Returns list of dicts with keys: name, type, sample_values.
    Same output format as detect_columns() but works without a DB.
    """
    import pandas as pd

    columns = []
    for col in df.columns:
        samples = [str(v) for v in df[col].dropna().head(3).tolist()]
        col_type = "text"
        if samples:
            try:
                [float(s) for s in samples]
                col_type = "numeric"
            except ValueError:
                pass
        columns.append({"name": col, "type": col_type, "sample_values": samples})
    return columns


def detect_columns(conn: Any, table_name: str) -> list[dict]:
    """Read column names, types, and sample values from a raw table.

    Returns list of dicts with keys: name, type, sample_values.
    """
    raw_name = raw_table_name(table_name)
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(
            f'SELECT * FROM "{raw_name}" LIMIT 5'
        )
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
    else:
        result = conn.execute(f'SELECT * FROM "{raw_name}" LIMIT 5')
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
    category_source = None
    name_lower = source_name.lower()
    for hint, cat in _NAME_CATEGORY_HINTS.items():
        if hint in name_lower:
            category = cat
            category_source = "name"
            break

    # If no category from name, try matching column names against profile hints
    if not category:
        for col_lower in col_names_lower:
            for abbrev, profile in EVIDENCE_CATEGORY_PROFILES.items():
                if any(h in col_lower for h in profile.column_hints):
                    category = abbrev
                    category_source = "column"
                    break
            if category:
                break

    # Guess centric: if we found chromosome/position → variant, else gene
    has_position = "chromosome" in fields and "position" in fields
    centric = "variant" if has_position else "gene"

    # Guess role: if source has trait + chr + pos columns, likely a locus source
    role = None
    if "trait" in fields and has_position:
        name_lower = source_name.lower()
        if "sumstat" in name_lower or "summary" in name_lower:
            role = "gwas_sumstats"
        else:
            role = "locus_definition"

    return {
        "fields": fields,
        "category": category,
        "centric": centric,
        "role": role,
    }


def validate_mapping(mapping: dict) -> list[str]:
    """Validate a proposed evidence mapping.

    Args:
        mapping: dict with keys: role OR (category, centric), plus source_tag, fields

    Returns:
        List of error strings (empty = valid).
    """
    errors = []

    role = mapping.get("role")
    if role:
        if role not in ("locus_definition", "gwas_sumstats"):
            errors.append(f"Unknown role '{role}'. Must be 'locus_definition' or 'gwas_sumstats'")

        if not mapping.get("source_tag"):
            errors.append("source_tag is required")

        fields = mapping.get("fields", {})
        # gene is optional for locus_definition (loci can be position-only)
        if role == "gwas_sumstats" and not fields.get("gene"):
            errors.append("Field mapping for 'gene' is required")
        if not fields.get("chromosome"):
            errors.append("Locus source requires 'chromosome' field")
        if not fields.get("position"):
            errors.append("Locus source requires 'position' field")
        if role == "locus_definition" and not fields.get("trait"):
            errors.append("Locus definition requires 'trait' field")

        return errors

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
    mappings: list[dict],
    config: dict,
    config_path: Path | None = None,
) -> dict:
    """Apply one or more evidence mappings to a raw table.

    1. Builds evidence blocks from the mappings
    2. Writes them to v2f.yaml (in place) if config_path provided
    3. Loads the raw table through evidence routing (once per block)
    4. Re-runs scoring once at the end

    Args:
        conn: Open database connection.
        source_name: Name of the raw table/source.
        mappings: List of mapping dicts (category, centric, source_tag, fields, etc.).
        config: Full resolved config dict.
        config_path: Path to v2f.yaml (for in-place modification). None = skip file update.

    Returns:
        Summary dict with results.
    """
    from pegasus_v2f.evidence_loader import load_evidence
    from pegasus_v2f.sources import _delete_evidence_by_source_tag, update_source_in_meta

    # Build evidence blocks from mappings
    evidence_blocks = []
    for mapping in mappings:
        evidence_blocks.append(_build_evidence_block(mapping))

    # Find the source in config
    sources = config.get("data_sources", [])
    source = next((s for s in sources if s["name"] == source_name), None)
    if not source:
        raise ValueError(f"Source '{source_name}' not found in config")

    # Update v2f.yaml in place
    if config_path and config_path.exists():
        _update_yaml_evidence_block(config_path, source_name, evidence_blocks)

    # Load raw data from the raw table (it's already in the DB)
    raw_name = raw_table_name(source_name)
    if is_postgres(conn):
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{raw_name}"')
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
    else:
        result = conn.execute(f'SELECT * FROM "{raw_name}"')
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()

    import pandas as pd
    df = pd.DataFrame(rows, columns=cols)

    # Ensure PEGASUS schema exists
    from pegasus_v2f.pegasus_schema import create_pegasus_schema
    create_pegasus_schema(conn)

    # Process each evidence block
    load_results = []
    for block in evidence_blocks:
        # Delete existing evidence rows for this source_tag (allows re-integration)
        source_tag = block.get("source_tag", "")
        if source_tag:
            _delete_evidence_by_source_tag(conn, source_tag)

        # Load through unified evidence loader
        load_result = load_evidence(conn, source, df, block)
        load_results.append(load_result)

    # Sync evidence blocks to _pegasus_meta (as a list)
    update_source_in_meta(conn, source_name, {"evidence": evidence_blocks})

    summary = {
        "source": source_name,
        "evidence_blocks": evidence_blocks,
        "load_results": load_results,
    }
    n = len(evidence_blocks)
    logger.info(f"Integrated '{source_name}' as {n} evidence {'entry' if n == 1 else 'entries'}")
    return summary


def _build_evidence_block(mapping: dict) -> dict:
    """Build an evidence block dict from a mapping dict."""
    if mapping.get("role"):
        block = {
            "role": mapping["role"],
            "source_tag": mapping["source_tag"],
            "fields": mapping["fields"],
        }
    else:
        block = {
            "category": mapping["category"],
            "source_tag": mapping["source_tag"],
            "fields": mapping["fields"],
        }
        if mapping.get("centric"):
            block["centric"] = mapping["centric"]
        if mapping.get("evidence_type"):
            block["evidence_type"] = mapping["evidence_type"]

    if mapping.get("study"):
        block["study"] = mapping["study"]
    if mapping.get("trait"):
        block["trait"] = mapping["trait"]

    return block


def _update_yaml_evidence_block(
    config_path: Path,
    source_name: str,
    evidence_blocks: list[dict],
) -> None:
    """Insert evidence blocks (list format) into v2f.yaml for the named source.

    Uses string manipulation to preserve comments and formatting.
    Always writes ``evidence:`` as a YAML list, even for a single block.
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
    last_prop_idx = source_line_idx  # track last non-blank property line
    while insert_idx < len(lines):
        line = lines[insert_idx]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            insert_idx += 1
            continue
        line_indent = len(line) - len(line.lstrip())
        # End of this source block: any line at base indent or less
        if line_indent <= base_indent:
            break
        # Check if evidence block already exists — remove it
        if stripped.startswith("evidence:"):
            logger.info(f"Source '{source_name}' already has evidence block — replacing")
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
            last_prop_idx = insert_idx - 1
            break
        last_prop_idx = insert_idx
        insert_idx += 1

    # Insert right after the last property line (not after trailing blank lines)
    insert_idx = last_prop_idx + 1

    # Build evidence YAML lines — always list format
    indent = " " * prop_indent
    item_indent = " " * (prop_indent + 2)       # "- " prefix for list items
    sub_indent = " " * (prop_indent + 4)         # continuation after "- "
    field_indent = " " * (prop_indent + 6)       # fields: values

    ev_lines = [f"{indent}evidence:"]
    for block in evidence_blocks:
        # First key of each block gets the "- " prefix
        first = True
        for key, value in _evidence_block_items(block):
            if key == "fields":
                if first:
                    ev_lines.append(f"{item_indent}- fields:")
                    first = False
                else:
                    ev_lines.append(f"{sub_indent}fields:")
                for fk, fv in value.items():
                    ev_lines.append(f"{field_indent}{fk}: {fv}")
            else:
                if first:
                    ev_lines.append(f"{item_indent}- {key}: {value}")
                    first = False
                else:
                    ev_lines.append(f"{sub_indent}{key}: {value}")

    # Insert before the next source block
    for ev_line in reversed(ev_lines):
        lines.insert(insert_idx, ev_line)

    config_path.write_text("\n".join(lines))
    n = len(evidence_blocks)
    logger.info(
        f"Updated {config_path} with {n} evidence "
        f"{'entry' if n == 1 else 'entries'} for '{source_name}'"
    )


def _evidence_block_items(block: dict):
    """Yield (key, value) pairs for an evidence block in canonical order."""
    # Role-based or category-based first
    if block.get("role"):
        yield "role", block["role"]
    else:
        yield "category", block["category"]
        yield "centric", block["centric"]
    if block.get("study"):
        yield "study", block["study"]
    if block.get("trait"):
        yield "trait", block["trait"]
    yield "source_tag", block["source_tag"]
    if block.get("evidence_type"):
        yield "evidence_type", block["evidence_type"]
    if block.get("fields"):
        yield "fields", block["fields"]
