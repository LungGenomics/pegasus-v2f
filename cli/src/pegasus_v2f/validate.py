"""Config validation against actual data — used by both configure and load."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from pegasus_v2f.inspect import _GENE_PATTERN
from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES


@dataclass
class ValidationCheck:
    field: str
    status: str  # "ok" | "warning" | "error"
    message: str
    fix_yaml: str | None = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "field": self.field,
            "status": self.status,
            "message": self.message,
        }
        if self.fix_yaml:
            d["fix_yaml"] = self.fix_yaml
        return d


@dataclass
class ValidationResult:
    name: str
    entity_type: str  # "source" | "study"
    checks: list[ValidationCheck]
    sample_rows: int

    @property
    def is_valid(self) -> bool:
        return not any(c.status == "error" for c in self.checks)

    @property
    def n_ok(self) -> int:
        return sum(1 for c in self.checks if c.status == "ok")

    @property
    def n_warnings(self) -> int:
        return sum(1 for c in self.checks if c.status == "warning")

    @property
    def n_errors(self) -> int:
        return sum(1 for c in self.checks if c.status == "error")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "entity_type": self.entity_type,
            "is_valid": self.is_valid,
            "checks": [c.to_dict() for c in self.checks],
            "sample_rows": self.sample_rows,
            "summary": {
                "ok": self.n_ok,
                "warnings": self.n_warnings,
                "errors": self.n_errors,
            },
        }


def validate_source(
    source_config: dict,
    data_dir: Path | None = None,
    df: pd.DataFrame | None = None,
) -> ValidationResult:
    """Validate a source config entry against its data file.

    If df is provided, uses it directly. Otherwise loads from source_config.
    """
    name = source_config.get("name", "unknown")
    checks: list[ValidationCheck] = []

    # Load data if not provided
    if df is None:
        try:
            from pegasus_v2f.loaders import load_source
            df = load_source(source_config, data_dir=data_dir)
            checks.append(ValidationCheck("file", "ok", f"File accessible: {_source_path(source_config)}"))
        except Exception as e:
            checks.append(ValidationCheck(
                "file", "error",
                f"Could not load data: {e}",
            ))
            return ValidationResult(name=name, entity_type="source", checks=checks, sample_rows=0)
    else:
        checks.append(ValidationCheck("file", "ok", f"Data loaded: {len(df)} rows"))

    # Apply transformations + clean_for_db so validation sees the same columns as add_source
    transformations = source_config.get("transformations", [])
    if transformations:
        from pegasus_v2f.transform import apply_transformations
        try:
            df = apply_transformations(df, transformations)
        except Exception as e:
            checks.append(ValidationCheck(
                "transformations", "warning",
                f"Could not apply transformations: {e}",
            ))

    from pegasus_v2f.transform import clean_for_db
    df = clean_for_db(df)

    sample_rows = min(1000, len(df))
    sample = df.head(sample_rows)

    # Check skip_rows
    skip = source_config.get("skip_rows")
    if skip is not None:
        if skip >= len(df):
            checks.append(ValidationCheck(
                "skip_rows", "error",
                f"skip_rows={skip} but data only has {len(df)} rows",
            ))
        else:
            checks.append(ValidationCheck("skip_rows", "ok", f"skip_rows={skip} valid"))

    # Check gene column — load_source renames gene_column -> "gene",
    # so the post-load df always has "gene" as the canonical column name.
    gene_col_config = source_config.get("gene_column", "gene")
    gene_col = "gene" if gene_col_config != "gene" and "gene" in df.columns else gene_col_config
    if gene_col in df.columns:
        non_null = df[gene_col].dropna().astype(str)
        valid = non_null.apply(lambda x: bool(_GENE_PATTERN.match(x)))
        valid_pct = valid.mean() * 100 if len(non_null) > 0 else 0
        null_count = int(df[gene_col].isna().sum())

        checks.append(ValidationCheck(
            "gene_column", "ok",
            f"Column '{gene_col}' found, {valid_pct:.1f}% valid HGNC symbols",
        ))
        if null_count > 0:
            checks.append(ValidationCheck(
                "gene_nulls", "warning",
                f"{null_count} rows have null gene values (will be dropped)",
            ))
    else:
        available = ", ".join(df.columns[:10])
        checks.append(ValidationCheck(
            "gene_column", "error",
            f"Column '{gene_col}' not found in data",
            fix_yaml=f"  gene_column: {_suggest_gene_col(df.columns)}" if _suggest_gene_col(df.columns) else None,
        ))
        if available:
            checks[-1].message += f"\n    Available columns: {available}"

    # Check evidence blocks
    evidence = source_config.get("evidence", [])
    for i, block in enumerate(evidence):
        cat = block.get("category")
        if cat and cat not in EVIDENCE_CATEGORIES:
            valid_cats = ", ".join(sorted(EVIDENCE_CATEGORIES.keys()))
            checks.append(ValidationCheck(
                f"evidence[{i}].category", "error",
                f"Category '{cat}' is not valid\n    Valid categories: {valid_cats}",
            ))
        elif cat:
            checks.append(ValidationCheck(
                f"evidence[{i}].category", "ok",
                f"Category '{cat}' is valid",
            ))

        # Check field mappings resolve to real columns
        fields = block.get("fields", {})
        for field_name, col_name in fields.items():
            # gene_column gets renamed to "gene" by load_source, so both
            # "gene" and the original gene_column name are valid references
            if col_name in df.columns:
                continue
            if col_name == "gene" or col_name == gene_col_config:
                continue
            checks.append(ValidationCheck(
                f"evidence[{i}].fields.{field_name}", "error",
                f"Field '{field_name}' maps to column '{col_name}' which is not in data",
            ))

    # Check transformations reference real columns
    for t in source_config.get("transformations", []):
        col = t.get("column")
        if col and col not in df.columns:
            checks.append(ValidationCheck(
                "transformation", "warning",
                f"Transformation references column '{col}' not found in data",
            ))

    return ValidationResult(
        name=name,
        entity_type="source",
        checks=checks,
        sample_rows=sample_rows,
    )


def validate_study(
    study_config: dict,
    locus_def: dict | None = None,
    data_dir: Path | None = None,
    df: pd.DataFrame | None = None,
) -> ValidationResult:
    """Validate a study config entry against its loci data."""
    name = study_config.get("id_prefix", "unknown")
    checks: list[ValidationCheck] = []

    # Load data if not provided
    if df is None:
        loci_source = study_config.get("loci_source")
        if not loci_source:
            checks.append(ValidationCheck("loci_source", "error", "No loci_source in config"))
            return ValidationResult(name=name, entity_type="study", checks=checks, sample_rows=0)

        try:
            df = _load_study_data(study_config, data_dir)
            checks.append(ValidationCheck("loci_file", "ok", f"Loci file accessible: {loci_source}"))
        except Exception as e:
            checks.append(ValidationCheck("loci_file", "error", f"Could not load loci data: {e}"))
            return ValidationResult(name=name, entity_type="study", checks=checks, sample_rows=0)
    else:
        checks.append(ValidationCheck("loci_file", "ok", f"Data loaded: {len(df)} rows"))

    # Apply transformations so validation sees post-transform columns
    transformations = study_config.get("transformations", [])
    if transformations:
        from pegasus_v2f.transform import apply_transformations
        try:
            df = apply_transformations(df, transformations)
        except Exception as e:
            checks.append(ValidationCheck(
                "transformations", "warning",
                f"Could not apply transformations: {e}",
            ))

    sample_rows = min(1000, len(df))

    # Normalize column names for checking
    col_lower = {c.lower(): c for c in df.columns}

    # Check chromosome column
    chr_col = _find_col(col_lower, ["chromosome", "chr", "chrom"])
    if chr_col:
        checks.append(ValidationCheck("chromosome", "ok", f"Chromosome column found: '{chr_col}'"))
        # Check chr format
        series = df[chr_col].dropna().astype(str)
        has_prefix = series.str.startswith("chr")
        if has_prefix.all():
            checks.append(ValidationCheck("chr_format", "warning", "All chromosomes have 'chr' prefix"))
        elif has_prefix.any() and not has_prefix.all():
            checks.append(ValidationCheck("chr_format", "warning", "Mixed chromosome formats"))
        else:
            checks.append(ValidationCheck("chr_format", "ok", "Consistent bare chromosome format"))
    else:
        checks.append(ValidationCheck(
            "chromosome", "error",
            "No chromosome column found (looked for: chromosome, chr, chrom)",
        ))

    # Check position column
    pos_col = _find_col(col_lower, ["position", "pos", "bp"])
    if pos_col:
        numeric = pd.to_numeric(df[pos_col], errors="coerce")
        valid_pct = numeric.notna().mean() * 100
        checks.append(ValidationCheck(
            "position", "ok",
            f"Position column found: '{pos_col}' ({valid_pct:.0f}% valid numeric)",
        ))
        if valid_pct < 95:
            checks.append(ValidationCheck(
                "position_validity", "warning",
                f"Only {valid_pct:.0f}% of positions are valid numbers",
            ))
    else:
        checks.append(ValidationCheck(
            "position", "error",
            "No position column found (looked for: position, pos, bp)",
        ))

    # Check traits
    traits = study_config.get("traits", [])
    if not traits:
        checks.append(ValidationCheck("traits", "error", "No traits specified"))
    else:
        checks.append(ValidationCheck("traits", "ok", f"Traits: {', '.join(traits)}"))

    # Check column mappings from config
    for config_key, desc in [
        ("gene_column", "Gene"),
        ("sentinel_column", "Sentinel"),
        ("pvalue_column", "P-value"),
        ("rsid_column", "rsID"),
    ]:
        col_name = study_config.get(config_key)
        if col_name and col_name.lower() not in col_lower:
            checks.append(ValidationCheck(
                config_key, "error",
                f"{desc} column '{col_name}' not found in data",
            ))

    # Check clustering would produce reasonable results
    if chr_col and pos_col:
        from pegasus_v2f.study_inspect import preview_clustering
        try:
            preview = preview_clustering(
                df, chr_col=chr_col, pos_col=pos_col,
                window_kb=locus_def.get("window_kb", 500) if locus_def else 500,
                merge_distance_kb=locus_def.get("merge_distance_kb", 250) if locus_def else 250,
            )
            if preview.n_loci > 0:
                checks.append(ValidationCheck(
                    "clustering", "ok",
                    f"Clustering: {preview.n_sentinels} sentinels -> {preview.n_loci} loci",
                ))
            else:
                checks.append(ValidationCheck(
                    "clustering", "warning",
                    "Clustering produced 0 loci",
                ))
        except Exception as e:
            checks.append(ValidationCheck("clustering", "warning", f"Clustering preview failed: {e}"))

    return ValidationResult(
        name=name,
        entity_type="study",
        checks=checks,
        sample_rows=sample_rows,
    )


def render_validation(result: ValidationResult, console: Any = None) -> None:
    """Rich-render validation results to stderr."""
    from rich.console import Console
    import sys

    if console is None:
        console = Console(stderr=True, file=sys.stderr)

    console.print(
        f"\n[bold]Validation: {result.name}[/bold] ({result.entity_type})"
    )
    console.print(f"  Sampled {result.sample_rows:,} rows\n")

    status_style = {"ok": "green", "warning": "yellow", "error": "red"}
    status_label = {"ok": "OK  ", "warning": "WARN", "error": "ERR "}

    for check in result.checks:
        style = status_style[check.status]
        label = status_label[check.status]
        console.print(f"  [{style}]{label}[/{style}] {check.message}")
        if check.fix_yaml:
            console.print(f"       Fix in v2f.yaml:")
            console.print(f"         {check.fix_yaml}")

    console.print(
        f"\n  Result: [{'green' if result.is_valid else 'red'}]"
        f"{'VALID' if result.is_valid else 'INVALID'}[/] "
        f"({result.n_ok} ok, {result.n_warnings} warning{'s' if result.n_warnings != 1 else ''}, "
        f"{result.n_errors} error{'s' if result.n_errors != 1 else ''})\n"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source_path(config: dict) -> str:
    """Extract human-readable path/URL from source config."""
    return config.get("path") or config.get("url") or config.get("name", "?")


def _suggest_gene_col(columns: pd.Index) -> str | None:
    """Suggest a gene column from available columns."""
    hints = ["gene", "gene_symbol", "gene_name", "symbol", "hgnc"]
    for hint in hints:
        for col in columns:
            if col.lower() == hint:
                return col
    return None


def _find_col(col_lower: dict[str, str], hints: list[str]) -> str | None:
    """Find a column by lowercase hint matching."""
    for hint in hints:
        if hint in col_lower:
            return col_lower[hint]
    return None


def _load_study_data(study_config: dict, data_dir: Path | None = None) -> pd.DataFrame:
    """Load study loci data from config."""
    loci_source = study_config["loci_source"]
    loci_sheet = study_config.get("loci_sheet")
    loci_skip = study_config.get("loci_skip")

    if loci_source.startswith("http"):
        from pegasus_v2f.loaders import load_googlesheets
        source_spec = {"url": loci_source, "source_type": "googlesheets"}
        if loci_sheet:
            source_spec["sheet"] = loci_sheet
        if loci_skip:
            source_spec["skip_rows"] = loci_skip
        return load_googlesheets(source_spec)

    path = Path(loci_source)
    if data_dir and not path.is_absolute():
        resolved = data_dir / path
        if resolved.exists():
            path = resolved

    if path.suffix.lower() in (".xlsx", ".xls"):
        kwargs: dict[str, Any] = {"engine": "calamine"}
        if loci_sheet:
            kwargs["sheet_name"] = loci_sheet
        if loci_skip:
            kwargs["skiprows"] = loci_skip
        return pd.read_excel(path, **kwargs)
    elif path.suffix.lower() in (".tsv", ".gz"):
        return pd.read_csv(path, sep="\t")
    else:
        return pd.read_csv(path)
