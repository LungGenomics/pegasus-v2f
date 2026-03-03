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
# Note: locus_definition "trait" requirement is conditional — see validate_evidence_config
REQUIRED_FIELDS = {
    "locus_definition": {"gene", "chromosome", "position"},
    "gwas_sumstats": {"chromosome", "position", "pvalue"},
    "gene": {"gene"},
    "variant": {"gene"},
}


def validate_pegasus_config(config: dict) -> list[str]:
    """Validate the pegasus: section of the config.

    Supports pegasus.study as a list of study dicts (multi-study).
    Returns list of error messages (empty = valid).
    """
    from pegasus_v2f.config import get_study_list

    errors = []
    pegasus = config.get("pegasus")
    if pegasus is None:
        return errors  # No pegasus section — nothing to validate

    # Study config — must be a non-empty list
    studies = get_study_list(config)
    if not studies:
        errors.append("pegasus.study is required when pegasus: section is present")
        return errors

    # Validate each study entry
    seen_prefixes: set[str] = set()
    all_traits_by_study: dict[str, list[str]] = {}
    for i, study in enumerate(studies):
        label = f"pegasus.study[{i}]"
        prefix = study.get("id_prefix")
        if not prefix:
            errors.append(f"{label}.id_prefix is required")
        else:
            if prefix in seen_prefixes:
                errors.append(f"{label}: duplicate id_prefix '{prefix}'")
            seen_prefixes.add(prefix)

        traits = study.get("traits")
        if not isinstance(traits, list) or len(traits) == 0:
            errors.append(f"{label}.traits must be a non-empty list of trait names")
        else:
            if prefix:
                all_traits_by_study[prefix] = traits

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

    # Cross-validate evidence.study and evidence.trait references
    multiple_studies = len(studies) > 1
    for src in sources:
        evidence = src.get("evidence")
        if not evidence:
            continue
        name = src.get("name", "<unnamed>")
        role = evidence.get("role")

        # evidence.study cross-validation
        ev_study = evidence.get("study")
        if ev_study:
            if ev_study not in seen_prefixes:
                errors.append(
                    f"source '{name}': evidence.study '{ev_study}' "
                    f"does not match any configured study id_prefix"
                )
        elif role in EVIDENCE_ROLES and multiple_studies:
            errors.append(
                f"source '{name}': evidence.study is required when multiple "
                f"studies are configured (has role '{role}')"
            )

        # evidence.trait cross-validation
        ev_trait = evidence.get("trait")
        if ev_trait:
            # Determine which study this source belongs to
            ref_study = ev_study or (studies[0].get("id_prefix") if len(studies) == 1 else None)
            if ref_study and ref_study in all_traits_by_study:
                if ev_trait not in all_traits_by_study[ref_study]:
                    errors.append(
                        f"source '{name}': evidence.trait '{ev_trait}' "
                        f"is not in study '{ref_study}' traits list"
                    )

        # gwas_sumstats requires evidence.trait
        if role == "gwas_sumstats" and not ev_trait:
            errors.append(
                f"source '{name}': evidence.trait is required for gwas_sumstats "
                f"(summary statistics are per-trait)"
            )

        # locus_definition without evidence.trait must have fields.trait mapping
        if role == "locus_definition" and not ev_trait:
            fields = evidence.get("fields", {})
            if "trait" not in fields:
                errors.append(
                    f"source '{name}': locus_definition without evidence.trait "
                    f"must have fields.trait mapping (trait from data column)"
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
        required = set(REQUIRED_FIELDS[role])
        # locus_definition: trait field mapping only required when evidence.trait is absent
        if role == "locus_definition" and not evidence.get("trait"):
            required.add("trait")
        missing = required - set(fields.keys())
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
