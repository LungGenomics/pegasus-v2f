"""Source data inspection — profile columns, validate genes, suggest fixes."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from pegasus_v2f.integrate import detect_columns_from_df, suggest_mappings
from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES, EVIDENCE_CATEGORY_PROFILES
from pegasus_v2f.report import Report

# Heuristic for plausible HGNC gene symbols
_GENE_PATTERN = re.compile(r"^[A-Z][A-Z0-9-]{0,14}$")


@dataclass
class ColumnProfile:
    name: str
    dtype: str  # "numeric", "text", "mixed"
    null_count: int
    null_pct: float
    unique_count: int
    sample_values: list[str]
    min_val: float | None = None
    max_val: float | None = None
    mean_val: float | None = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "name": self.name,
            "dtype": self.dtype,
            "null_count": self.null_count,
            "null_pct": round(self.null_pct, 2),
            "unique_count": self.unique_count,
            "sample_values": self.sample_values,
        }
        if self.dtype == "numeric":
            d["min"] = self.min_val
            d["max"] = self.max_val
            d["mean"] = round(self.mean_val, 4) if self.mean_val is not None else None
        return d


@dataclass
class GeneAnalysis:
    column_name: str
    total_values: int
    null_count: int
    unique_count: int
    valid_hgnc_pct: float
    sample_valid: list[str]
    sample_invalid: list[str]

    def to_dict(self) -> dict:
        return {
            "column_name": self.column_name,
            "total_values": self.total_values,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "valid_hgnc_pct": round(self.valid_hgnc_pct, 2),
            "sample_valid": self.sample_valid,
            "sample_invalid": self.sample_invalid,
        }


@dataclass
class ChromosomeAnalysis:
    column_name: str
    has_chr_prefix: bool | None  # True=all, False=none, None=mixed
    unique_values: list[str]
    format_consistency: str  # "consistent_chr", "consistent_bare", "mixed"

    def to_dict(self) -> dict:
        return {
            "column_name": self.column_name,
            "has_chr_prefix": self.has_chr_prefix,
            "unique_values": self.unique_values,
            "format_consistency": self.format_consistency,
        }


@dataclass
class SuggestedFix:
    code: str
    message: str
    transformation: dict | None = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.transformation:
            d["transformation"] = self.transformation
        return d


@dataclass
class InspectionResult:
    source_name: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]
    gene_analysis: GeneAnalysis | None
    chromosome_analysis: ChromosomeAnalysis | None
    suggested_mappings: dict
    suggested_fixes: list[SuggestedFix]
    compatibility_score: float
    pegasus_categories: dict = field(default_factory=lambda: EVIDENCE_CATEGORIES)

    def to_dict(self) -> dict:
        return {
            "source_name": self.source_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.to_dict() for c in self.columns],
            "gene_analysis": self.gene_analysis.to_dict() if self.gene_analysis else None,
            "chromosome_analysis": (
                self.chromosome_analysis.to_dict() if self.chromosome_analysis else None
            ),
            "suggested_mappings": self.suggested_mappings,
            "suggested_fixes": [f.to_dict() for f in self.suggested_fixes],
            "compatibility_score": round(self.compatibility_score, 2),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Core inspection
# ---------------------------------------------------------------------------


def inspect_dataframe(
    df: pd.DataFrame,
    source_name: str = "",
    gene_col: str | None = None,
    chr_col: str | None = None,
    known_genes: set[str] | None = None,
) -> InspectionResult:
    """Full inspection of a DataFrame for PEGASUS compatibility."""
    columns = _profile_columns(df)
    col_dicts = detect_columns_from_df(df)
    mappings = suggest_mappings(col_dicts, source_name)

    # Auto-detect gene/chr columns from suggestions if not specified
    if gene_col is None:
        gene_col = mappings["fields"].get("gene")
    if chr_col is None:
        chr_col = mappings["fields"].get("chromosome")

    gene_analysis = _analyze_genes(df, gene_col, known_genes) if gene_col and gene_col in df.columns else None
    chr_analysis = _analyze_chromosomes(df, chr_col) if chr_col and chr_col in df.columns else None

    fixes: list[SuggestedFix] = []

    # Chromosome fixes
    if chr_analysis and chr_analysis.has_chr_prefix is True:
        fixes.append(SuggestedFix(
            "strip_chr_prefix",
            f"Chromosome column '{chr_col}' has 'chr' prefix. "
            f"Strip to match bare format (1, 2, ..., X) used by most sources.",
            {"type": "strip_prefix", "column": chr_col, "prefix": "chr"},
        ))
    elif chr_analysis and chr_analysis.has_chr_prefix is None:
        fixes.append(SuggestedFix(
            "mixed_chr_prefix",
            f"Chromosome column '{chr_col}' has mixed formats (some with 'chr', some without).",
            None,
        ))

    # Gene fixes
    if gene_analysis:
        if gene_analysis.null_count > 0:
            fixes.append(SuggestedFix(
                "drop_null_genes",
                f"{gene_analysis.null_count} rows have empty gene column. Drop before loading.",
                {"type": "drop_nulls", "column": gene_col},
            ))
        if gene_analysis.valid_hgnc_pct < 100 and gene_analysis.sample_invalid:
            # Check if it's a case issue
            non_null = df[gene_col].dropna()
            upper_match_pct = non_null.apply(
                lambda x: bool(_GENE_PATTERN.match(str(x).upper()))
            ).mean() * 100
            if upper_match_pct > gene_analysis.valid_hgnc_pct + 5:
                fixes.append(SuggestedFix(
                    "normalize_gene_case",
                    f"Gene column '{gene_col}' has mixed case. Uppercase would improve HGNC match.",
                    {"type": "uppercase", "column": gene_col},
                ))

    # Numeric coercion fixes
    for cp in columns:
        if cp.dtype == "mixed":
            fixes.append(SuggestedFix(
                "coerce_numeric",
                f"Column '{cp.name}' has mixed types. Coerce to numeric.",
                {"type": "coerce_numeric", "column": cp.name},
            ))

    score = _compute_compatibility_score(gene_analysis, chr_analysis, columns, mappings)

    return InspectionResult(
        source_name=source_name,
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        gene_analysis=gene_analysis,
        chromosome_analysis=chr_analysis,
        suggested_mappings=mappings,
        suggested_fixes=fixes,
        compatibility_score=score,
    )


# ---------------------------------------------------------------------------
# Column profiling
# ---------------------------------------------------------------------------


def _profile_columns(df: pd.DataFrame) -> list[ColumnProfile]:
    profiles = []
    for col in df.columns:
        series = df[col]
        null_count = int(series.isna().sum())
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0.0
        unique_count = int(series.nunique())
        samples = [str(v) for v in series.dropna().head(5).tolist()]

        # Determine dtype
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = series.dropna()
        numeric_non_null = numeric.dropna()

        if len(non_null) == 0:
            dtype = "text"
            min_val = max_val = mean_val = None
        elif len(numeric_non_null) == len(non_null):
            dtype = "numeric"
            min_val = float(numeric_non_null.min())
            max_val = float(numeric_non_null.max())
            mean_val = float(numeric_non_null.mean())
        elif len(numeric_non_null) > len(non_null) * 0.5:
            dtype = "mixed"
            min_val = float(numeric_non_null.min())
            max_val = float(numeric_non_null.max())
            mean_val = float(numeric_non_null.mean())
        else:
            dtype = "text"
            min_val = max_val = mean_val = None

        profiles.append(ColumnProfile(
            name=col, dtype=dtype,
            null_count=null_count, null_pct=null_pct,
            unique_count=unique_count, sample_values=samples,
            min_val=min_val, max_val=max_val, mean_val=mean_val,
        ))
    return profiles


def _analyze_genes(
    df: pd.DataFrame, gene_col: str, known_genes: set[str] | None = None
) -> GeneAnalysis:
    series = df[gene_col]
    null_count = int(series.isna().sum())
    non_null = series.dropna().astype(str)
    unique_count = int(non_null.nunique())

    if known_genes:
        valid = non_null[non_null.isin(known_genes)]
        invalid = non_null[~non_null.isin(known_genes)]
    else:
        valid = non_null[non_null.apply(lambda x: bool(_GENE_PATTERN.match(x)))]
        invalid = non_null[~non_null.apply(lambda x: bool(_GENE_PATTERN.match(x)))]

    valid_pct = (len(valid) / len(non_null) * 100) if len(non_null) > 0 else 0.0

    return GeneAnalysis(
        column_name=gene_col,
        total_values=len(series),
        null_count=null_count,
        unique_count=unique_count,
        valid_hgnc_pct=valid_pct,
        sample_valid=valid.head(5).tolist(),
        sample_invalid=invalid.head(5).tolist(),
    )


def _analyze_chromosomes(df: pd.DataFrame, chr_col: str) -> ChromosomeAnalysis:
    series = df[chr_col].dropna().astype(str)
    unique = sorted(series.unique().tolist())

    with_prefix = series.str.startswith("chr")
    all_prefixed = bool(with_prefix.all())
    none_prefixed = bool((~with_prefix).all())

    if all_prefixed:
        has_chr = True
        consistency = "consistent_chr"
    elif none_prefixed:
        has_chr = False
        consistency = "consistent_bare"
    else:
        has_chr = None
        consistency = "mixed"

    return ChromosomeAnalysis(
        column_name=chr_col,
        has_chr_prefix=has_chr,
        unique_values=unique[:15],
        format_consistency=consistency,
    )


def _compute_compatibility_score(
    gene_analysis: GeneAnalysis | None,
    chr_analysis: ChromosomeAnalysis | None,
    columns: list[ColumnProfile],
    mappings: dict,
) -> float:
    score = 0.0

    # Has identifiable gene column (0.25)
    if gene_analysis:
        score += 0.25

    # Gene validation rate > 90% (0.20)
    if gene_analysis and gene_analysis.valid_hgnc_pct > 90:
        score += 0.20

    # Has evidence-type column (0.20)
    evidence_fields = {"pvalue", "score", "effect_size"}
    if any(f in mappings.get("fields", {}) for f in evidence_fields):
        score += 0.20

    # Chromosome format consistent (0.10)
    if chr_analysis and chr_analysis.format_consistency != "mixed":
        score += 0.10

    # Null rate < 5% for key columns (0.10)
    key_cols = [c for c in columns if c.name in mappings.get("fields", {}).values()]
    if key_cols and all(c.null_pct < 5 for c in key_cols):
        score += 0.10

    # Column names match PEGASUS field patterns (0.15)
    matched_fields = len(mappings.get("fields", {}))
    if matched_fields >= 3:
        score += 0.15
    elif matched_fields >= 2:
        score += 0.10
    elif matched_fields >= 1:
        score += 0.05

    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Report conversion and rendering
# ---------------------------------------------------------------------------


def inspection_to_report(result: InspectionResult) -> Report:
    """Convert InspectionResult to a Report for rendering via render_report()."""
    report = Report(operation=f"inspect: {result.source_name}")
    report.counters["rows"] = result.row_count
    report.counters["columns"] = result.column_count

    if result.gene_analysis:
        ga = result.gene_analysis
        report.info(
            "gene_validation",
            f"{ga.valid_hgnc_pct:.1f}% valid HGNC format "
            f"({ga.unique_count} unique, {ga.null_count} null)",
        )
        if ga.sample_invalid:
            report.warning(
                "invalid_genes",
                f"Sample invalid: {', '.join(ga.sample_invalid[:5])}",
                count=len(ga.sample_invalid),
            )

    if result.chromosome_analysis:
        ca = result.chromosome_analysis
        if ca.format_consistency == "mixed":
            report.warning("mixed_chr_format", "Mixed chromosome formats (some with 'chr', some without)")
        elif ca.has_chr_prefix:
            report.info("chr_prefix", "All chromosomes have 'chr' prefix")

    for fix in result.suggested_fixes:
        report.warning(fix.code, fix.message)

    mappings = result.suggested_mappings
    if mappings.get("category"):
        report.info("suggested_category", f"Category: {mappings['category']}")
    report.info("centric", f"Centric: {mappings.get('centric', 'unknown')}")

    return report


def render_inspection(result: InspectionResult, console: Any = None) -> None:
    """Rich-render inspection results to stderr."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    if console is None:
        import sys
        console = Console(stderr=True, file=sys.stderr)

    # Header
    console.print(f"\n[bold]Inspection: {result.source_name}[/bold] "
                  f"({result.row_count:,} rows, {result.column_count} columns)\n")

    # Column table
    col_table = Table(title="Columns")
    col_table.add_column("Column", style="bold")
    col_table.add_column("Type")
    col_table.add_column("Nulls")
    col_table.add_column("Samples")
    for cp in result.columns:
        null_str = f"{cp.null_count} ({cp.null_pct:.0f}%)"
        col_table.add_row(cp.name, cp.dtype, null_str, ", ".join(cp.sample_values[:3]))
    console.print(col_table)

    # Gene analysis
    if result.gene_analysis:
        ga = result.gene_analysis
        non_null = ga.total_values - ga.null_count
        console.print(f"\n  [bold]Gene Validation[/bold] ({ga.column_name})")
        console.print(f"    {non_null - len(ga.sample_invalid):,}/{non_null:,} "
                      f"({ga.valid_hgnc_pct:.1f}%) valid HGNC format")
        if ga.sample_invalid:
            console.print(f"    [yellow]Sample invalid: {', '.join(ga.sample_invalid[:5])}[/yellow]")

    # Chromosome analysis
    if result.chromosome_analysis:
        ca = result.chromosome_analysis
        console.print(f"\n  [bold]Chromosome Format[/bold]")
        if ca.has_chr_prefix is True:
            console.print("    [yellow]! All values have 'chr' prefix[/yellow]")
        elif ca.has_chr_prefix is None:
            console.print("    [yellow]! Mixed formats (some with 'chr', some without)[/yellow]")
        else:
            console.print("    [green]Consistent bare format (no prefix)[/green]")

    # Suggested mappings
    mappings = result.suggested_mappings
    if mappings.get("fields"):
        console.print(f"\n  [bold]Suggested Mappings[/bold]")
        fields_str = ", ".join(f"{k} -> {v}" for k, v in mappings["fields"].items())
        console.print(f"    {fields_str}")
        if mappings.get("category"):
            cat = mappings["category"]
            label = EVIDENCE_CATEGORIES.get(cat, "")
            console.print(f"    Category: {cat} ({label})")
        console.print(f"    Centric: {mappings.get('centric', 'unknown')}")

    # Suggested fixes
    if result.suggested_fixes:
        console.print(f"\n  [bold]Suggested Fixes[/bold]")
        for i, fix in enumerate(result.suggested_fixes, 1):
            console.print(f"    {i}. [yellow]{fix.code}[/yellow]: {fix.message}")

    # Compatibility score
    score = result.compatibility_score
    style = "green" if score >= 0.7 else "yellow" if score >= 0.4 else "red"
    console.print(f"\n  [bold]Compatibility:[/bold] [{style}]{score:.2f} / 1.0[/{style}]\n")
