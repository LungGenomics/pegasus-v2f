"""Evidence config validation — validates PEGASUS study config and evidence blocks."""

from __future__ import annotations

import logging

from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES

logger = logging.getLogger(__name__)

# Valid evidence roles (special source behaviors)
EVIDENCE_ROLES = {"locus_definition", "gwas_sumstats"}

# Valid centric types (for non-role evidence sources)
EVIDENCE_CENTRICS = {"gene", "variant"}

# Required field mappings per role / centric
REQUIRED_FIELDS = {
    "locus_definition": {"gene", "trait", "chromosome", "position"},
    "gwas_sumstats": {"chromosome", "position", "pvalue"},
    "gene": {"gene"},
    "variant": {"gene"},
}


def validate_pegasus_config(config: dict) -> list[str]:
    """Validate the pegasus: section of the config.

    Returns list of error messages (empty = valid).
    """
    errors = []
    pegasus = config.get("pegasus")
    if pegasus is None:
        return errors  # No pegasus section — nothing to validate

    # Study config
    study = pegasus.get("study")
    if not study:
        errors.append("pegasus.study is required when pegasus: section is present")
        return errors

    if "id_prefix" not in study:
        errors.append("pegasus.study.id_prefix is required")

    traits = study.get("traits")
    if not isinstance(traits, list) or len(traits) == 0:
        errors.append("pegasus.study.traits must be a non-empty list of trait names")

    # Check that at least one locus source exists
    sources = config.get("data_sources", [])
    has_locus_source = any(
        s.get("evidence", {}).get("role") in EVIDENCE_ROLES
        for s in sources
    )
    if not has_locus_source:
        errors.append(
            "pegasus.study is declared but no data source has "
            "role: locus_definition or role: gwas_sumstats. "
            "At least one locus source is required."
        )

    # Integration config (optional but validated if present)
    integration = pegasus.get("integration")
    if integration:
        method = integration.get("method")
        if method and method != "criteria_count_v1":
            errors.append(
                f"pegasus.integration.method '{method}' is not recognized "
                f"(supported: criteria_count_v1)"
            )

    return errors


def validate_evidence_config(source: dict) -> list[str]:
    """Validate the evidence: block of a single data source.

    Returns list of error messages (empty = valid).
    """
    errors = []
    evidence = source.get("evidence")
    if not evidence:
        return errors  # No evidence block — raw source, skip

    name = source.get("name", "<unnamed>")
    role = evidence.get("role")
    centric = evidence.get("centric")

    # Must have either role or (category + centric)
    if role:
        if role not in EVIDENCE_ROLES:
            errors.append(
                f"source '{name}': evidence.role '{role}' is not valid "
                f"(must be one of: {', '.join(sorted(EVIDENCE_ROLES))})"
            )

        # source_tag required for roles
        if "source_tag" not in evidence:
            errors.append(f"source '{name}': evidence.source_tag is required for role sources")

        # Validate sumstats-specific fields
        if role == "gwas_sumstats":
            pval = evidence.get("pvalue_threshold")
            if pval is not None:
                try:
                    float(pval)
                except (TypeError, ValueError):
                    errors.append(
                        f"source '{name}': evidence.pvalue_threshold must be numeric"
                    )

    elif centric:
        if centric not in EVIDENCE_CENTRICS:
            errors.append(
                f"source '{name}': evidence.centric '{centric}' is not valid "
                f"(must be one of: {', '.join(sorted(EVIDENCE_CENTRICS))})"
            )

        category = evidence.get("category")
        if not category:
            errors.append(f"source '{name}': evidence.category is required for centric sources")
        elif category not in EVIDENCE_CATEGORIES:
            errors.append(
                f"source '{name}': evidence.category '{category}' is not a valid "
                f"PEGASUS category (valid: {', '.join(sorted(EVIDENCE_CATEGORIES))})"
            )

        if "source_tag" not in evidence:
            errors.append(f"source '{name}': evidence.source_tag is required")

    else:
        errors.append(
            f"source '{name}': evidence block must have either 'role' or 'centric' "
            f"(with 'category')"
        )

    # Validate field mappings
    fields = evidence.get("fields", {})
    if role and role in REQUIRED_FIELDS:
        missing = REQUIRED_FIELDS[role] - set(fields.keys())
        if missing:
            errors.append(
                f"source '{name}': evidence.fields missing required mappings "
                f"for role '{role}': {', '.join(sorted(missing))}"
            )
    elif centric and centric in REQUIRED_FIELDS:
        missing = REQUIRED_FIELDS[centric] - set(fields.keys())
        if missing:
            errors.append(
                f"source '{name}': evidence.fields missing required mappings "
                f"for centric '{centric}': {', '.join(sorted(missing))}"
            )

    return errors


def resolve_evidence_mapping(source: dict, df) -> dict:
    """Resolve column mappings from evidence config against a DataFrame.

    Returns a dict mapping logical field names to actual DataFrame column names.
    Raises ValueError if a mapped column doesn't exist in the DataFrame.
    """
    evidence = source.get("evidence", {})
    fields = evidence.get("fields", {})

    mapping = {}
    missing = []

    for logical_name, col_name in fields.items():
        if col_name in df.columns:
            mapping[logical_name] = col_name
        else:
            missing.append(f"{logical_name} → '{col_name}'")

    if missing:
        name = source.get("name", "<unnamed>")
        raise ValueError(
            f"Source '{name}': evidence field mappings reference columns not found "
            f"in data: {', '.join(missing)}. "
            f"Available columns: {', '.join(df.columns.tolist())}"
        )

    return mapping
