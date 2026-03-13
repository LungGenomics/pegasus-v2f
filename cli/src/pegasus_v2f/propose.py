"""Config proposal — generate v2f.yaml entries from data inspection."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from pegasus_v2f.ai_assist import AISuggestion
from pegasus_v2f.inspect import InspectionResult, SuggestedFix, inspect_dataframe
from pegasus_v2f.integrate import suggest_mappings, detect_columns_from_df
from pegasus_v2f.study_inspect import (
    StudyInspectionResult,
    inspect_sentinels,
)


def propose_source_config(
    df: pd.DataFrame,
    source_name: str,
    source_def: dict,
    *,
    ai_suggestion: AISuggestion | None = None,
    gene_column: str | None = None,
    category: str | None = None,
    traits: str | None = None,
    centric: str | None = None,
    source_tag: str | None = None,
) -> tuple[dict, InspectionResult]:
    """Generate a complete data_sources entry for v2f.yaml.

    Returns (source_config, inspection_result).
    """
    # Apply any user-provided transforms before inspecting, so we see
    # the post-transform columns (e.g., rename creates gene column).
    user_transforms = source_def.get("transformations", [])
    if user_transforms:
        from pegasus_v2f.transform import apply_transformations
        df = apply_transformations(df, user_transforms)

    inspection = inspect_dataframe(df, source_name=source_name)
    mappings = inspection.suggested_mappings

    # Determine gene column
    resolved_gene = gene_column
    if not resolved_gene and ai_suggestion and ai_suggestion.column_mappings:
        resolved_gene = ai_suggestion.column_mappings.get("gene")
    if not resolved_gene:
        resolved_gene = mappings["fields"].get("gene")

    # Determine category
    resolved_category = category
    if not resolved_category and ai_suggestion:
        resolved_category = ai_suggestion.category
    if not resolved_category:
        resolved_category = mappings.get("category")

    # Determine centric
    resolved_centric = centric
    if not resolved_centric and ai_suggestion:
        resolved_centric = ai_suggestion.centric
    if not resolved_centric:
        resolved_centric = mappings.get("centric", "gene")

    # Build source config
    config = dict(source_def)
    config["name"] = source_name

    if resolved_gene and resolved_gene != "gene":
        config["gene_column"] = resolved_gene

    # Collect transformations: start with any user-provided transforms from source_def,
    # then append heuristic suggestions that don't duplicate existing ones.
    existing_transforms = list(config.get("transformations", []))
    existing_types = {(t.get("type"), t.get("column")) for t in existing_transforms}

    for fix in inspection.suggested_fixes:
        if fix.transformation:
            key = (fix.transformation.get("type"), fix.transformation.get("column"))
            if key not in existing_types:
                existing_transforms.append(fix.transformation)

    # AI transformations override all heuristic ones (but keep user-provided)
    if ai_suggestion and ai_suggestion.transformations:
        user_transforms = list(source_def.get("transformations", []))
        existing_transforms = user_transforms + list(ai_suggestion.transformations)

    if existing_transforms:
        config["transformations"] = existing_transforms

    # Build evidence blocks
    if resolved_category and resolved_gene:
        evidence_blocks = _build_evidence_blocks(
            source_name=source_name,
            mappings=mappings,
            gene_column=resolved_gene,
            category=resolved_category,
            centric=resolved_centric,
            source_tag=source_tag,
            traits=traits,
            ai_suggestion=ai_suggestion,
        )
        if evidence_blocks:
            config["evidence"] = evidence_blocks

    return config, inspection


def _build_evidence_blocks(
    *,
    source_name: str,
    mappings: dict,
    gene_column: str,
    category: str,
    centric: str,
    source_tag: str | None,
    traits: str | None,
    ai_suggestion: AISuggestion | None,
) -> list[dict]:
    """Build evidence blocks from inspection mappings."""
    # Evidence fields reference post-load column names.
    # load_source renames gene_column -> "gene", so always use "gene" here.
    fields = {"gene": "gene"}

    # Add variant-level fields if variant-centric
    if centric == "variant":
        if mappings["fields"].get("chromosome"):
            fields["chromosome"] = mappings["fields"]["chromosome"]
        if mappings["fields"].get("position"):
            fields["position"] = mappings["fields"]["position"]
        if mappings["fields"].get("rsid"):
            fields["rsid"] = mappings["fields"]["rsid"]

    # Detect evidence columns (pvalue, score, effect_size)
    evidence_fields = {}
    for field_type in ("pvalue", "score", "effect_size"):
        if mappings["fields"].get(field_type):
            evidence_fields[field_type] = mappings["fields"][field_type]

    if not evidence_fields:
        # Single block with gene-only mapping
        tag = source_tag or source_name
        block = {
            "source_tag": tag,
            "category": category,
            "centric": centric,
            "fields": fields,
        }
        if traits:
            block["traits"] = [t.strip() for t in traits.split(",") if t.strip()]
        return [block]

    # One block per evidence column
    blocks = []
    for field_type, col_name in evidence_fields.items():
        sanitized = re.sub(r"[^a-zA-Z0-9]", "_", col_name).strip("_").lower()
        tag = source_tag or f"{source_name}_{sanitized}"
        block_fields = dict(fields)
        block_fields[field_type] = col_name

        block = {
            "source_tag": tag,
            "category": category,
            "centric": centric,
            "fields": block_fields,
        }
        if traits:
            block["traits"] = [t.strip() for t in traits.split(",") if t.strip()]
        blocks.append(block)

    return blocks


def propose_study_config(
    df: pd.DataFrame,
    study_name: str,
    traits: list[str],
    loci_source: str,
    *,
    loci_sheet: str | None = None,
    loci_skip: int | None = None,
    window_kb: int = 500,
    merge_distance_kb: int = 250,
    ai_suggestion: AISuggestion | None = None,
    cache_dir: Path | None = None,
    gene_column: str | None = None,
    sentinel_column: str | None = None,
    pvalue_column: str | None = None,
    rsid_column: str | None = None,
    gwas_source: str | None = None,
    ancestry: str | None = None,
    sex: str | None = None,
    sample_size: int | None = None,
    doi: str | None = None,
    year: int | None = None,
) -> tuple[dict, StudyInspectionResult]:
    """Generate a complete pegasus.study entry for v2f.yaml.

    Returns (study_config, inspection_result).
    """
    inspection = inspect_sentinels(
        df,
        source_label=study_name,
        window_kb=window_kb,
        merge_distance_kb=merge_distance_kb,
        cache_dir=cache_dir,
        gene_col=gene_column,
        pvalue_col=pvalue_column,
        rsid_col=rsid_column,
        sentinel_col=sentinel_column,
    )
    det = inspection.column_detection

    # Resolve columns from detection or AI
    resolved_gene = gene_column or det.gene
    resolved_sentinel = sentinel_column or det.sentinel_id
    resolved_pvalue = pvalue_column or det.pvalue
    resolved_rsid = rsid_column or det.rsid

    if ai_suggestion and ai_suggestion.column_mappings:
        m = ai_suggestion.column_mappings
        if not resolved_gene and m.get("gene"):
            resolved_gene = m["gene"]
        if not resolved_sentinel and m.get("sentinel_id"):
            resolved_sentinel = m["sentinel_id"]
        if not resolved_pvalue and m.get("pvalue"):
            resolved_pvalue = m["pvalue"]
        if not resolved_rsid and m.get("rsid"):
            resolved_rsid = m["rsid"]

    # Build study config
    config: dict[str, Any] = {
        "id_prefix": study_name,
        "traits": traits,
        "loci_source": loci_source,
    }
    if loci_sheet:
        config["loci_sheet"] = loci_sheet
    if loci_skip:
        config["loci_skip"] = loci_skip
    if resolved_gene:
        config["gene_column"] = resolved_gene
    if resolved_sentinel:
        config["sentinel_column"] = resolved_sentinel
    if resolved_pvalue:
        config["pvalue_column"] = resolved_pvalue
    if resolved_rsid:
        config["rsid_column"] = resolved_rsid
    if gwas_source:
        config["gwas_source"] = gwas_source
    if ancestry:
        config["ancestry"] = ancestry
    if sex:
        config["sex"] = sex
    if sample_size:
        config["sample_size"] = sample_size
    if doi:
        config["doi"] = doi
    if year:
        config["year"] = year

    # Collect transformations
    transformations = []
    for fix in inspection.suggested_fixes:
        if fix.transformation:
            transformations.append(fix.transformation)
    if ai_suggestion and ai_suggestion.transformations:
        transformations = list(ai_suggestion.transformations)
    if transformations:
        config["transformations"] = transformations

    return config, inspection
