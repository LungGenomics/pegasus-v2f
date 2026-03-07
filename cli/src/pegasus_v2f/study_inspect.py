"""Sentinel data inspection — profile columns, validate positions, preview clustering."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from pegasus_v2f.inspect import (
    ColumnProfile,
    ChromosomeAnalysis,
    GeneAnalysis,
    SuggestedFix,
    _analyze_chromosomes,
    _analyze_genes,
    _profile_columns,
)
from pegasus_v2f.report import Report
from pegasus_v2f.study_management import (
    DEFAULT_MERGE_DISTANCE_KB,
    DEFAULT_WINDOW_KB,
    _cluster_sentinels,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column detection heuristics
# ---------------------------------------------------------------------------

_SENTINEL_COLUMN_HINTS: dict[str, list[str]] = {
    "chromosome": ["chromosome", "chr", "chrom"],
    "position": ["position", "pos", "bp", "base_pair", "basepair"],
    "trait": ["trait", "phenotype", "pheno"],
    "gene": ["gene", "gene_symbol", "nearest_gene", "gene_name", "symbol"],
    "pvalue": ["pvalue", "p_value", "p.value", "pval", "p", "min_p"],
    "rsid": ["rsid", "rs_id", "snp", "rs", "dbsnp"],
    "sentinel_id": ["variant_id", "sentinel", "variant", "lead_variant", "varid"],
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PositionAnalysis:
    total_rows: int
    valid_count: int
    invalid_count: int
    invalid_samples: list[str]
    min_position: int | None
    max_position: int | None

    def to_dict(self) -> dict:
        return {
            "total_rows": self.total_rows,
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "invalid_samples": self.invalid_samples,
            "min_position": self.min_position,
            "max_position": self.max_position,
        }


@dataclass
class TraitAnalysis:
    column_name: str
    unique_traits: list[str]
    counts: dict[str, int]

    def to_dict(self) -> dict:
        return {
            "column_name": self.column_name,
            "unique_traits": self.unique_traits,
            "counts": self.counts,
        }


@dataclass
class LocusPreview:
    chromosome: str
    start: int
    end: int
    n_sentinels: int
    locus_name: str
    sample_positions: list[int]

    def to_dict(self) -> dict:
        return {
            "chromosome": self.chromosome,
            "start": self.start,
            "end": self.end,
            "n_sentinels": self.n_sentinels,
            "locus_name": self.locus_name,
            "sample_positions": self.sample_positions,
        }


@dataclass
class ClusteringPreview:
    n_sentinels: int
    n_loci: int
    window_kb: int
    merge_distance_kb: int
    loci: list[LocusPreview]
    by_chromosome: dict[str, int]
    by_trait: dict[str, int] | None = None  # trait → n_loci (when trait column present)

    def to_dict(self) -> dict:
        d = {
            "n_sentinels": self.n_sentinels,
            "n_loci": self.n_loci,
            "window_kb": self.window_kb,
            "merge_distance_kb": self.merge_distance_kb,
            "loci": [lp.to_dict() for lp in self.loci],
            "by_chromosome": self.by_chromosome,
        }
        if self.by_trait is not None:
            d["by_trait"] = self.by_trait
        return d


@dataclass
class SentinelColumnDetection:
    chromosome: str | None = None
    position: str | None = None
    trait: str | None = None
    gene: str | None = None
    pvalue: str | None = None
    rsid: str | None = None
    sentinel_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "chromosome": self.chromosome,
            "position": self.position,
            "trait": self.trait,
            "gene": self.gene,
            "pvalue": self.pvalue,
            "rsid": self.rsid,
            "sentinel_id": self.sentinel_id,
        }


@dataclass
class StudyInspectionResult:
    source_label: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]
    column_detection: SentinelColumnDetection
    chromosome_analysis: ChromosomeAnalysis | None
    position_analysis: PositionAnalysis | None
    gene_analysis: GeneAnalysis | None
    trait_analysis: TraitAnalysis | None
    clustering_preview: ClusteringPreview | None
    suggested_fixes: list[SuggestedFix]
    readiness_score: float

    def to_dict(self) -> dict:
        return {
            "source_label": self.source_label,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.to_dict() for c in self.columns],
            "column_detection": self.column_detection.to_dict(),
            "chromosome_analysis": (
                self.chromosome_analysis.to_dict() if self.chromosome_analysis else None
            ),
            "position_analysis": (
                self.position_analysis.to_dict() if self.position_analysis else None
            ),
            "gene_analysis": self.gene_analysis.to_dict() if self.gene_analysis else None,
            "trait_analysis": self.trait_analysis.to_dict() if self.trait_analysis else None,
            "clustering_preview": (
                self.clustering_preview.to_dict() if self.clustering_preview else None
            ),
            "suggested_fixes": [f.to_dict() for f in self.suggested_fixes],
            "readiness_score": round(self.readiness_score, 2),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Core inspection
# ---------------------------------------------------------------------------


def inspect_sentinels(
    df: pd.DataFrame,
    source_label: str = "",
    *,
    window_kb: int = DEFAULT_WINDOW_KB,
    merge_distance_kb: int = DEFAULT_MERGE_DISTANCE_KB,
    cache_dir: Path | None = None,
    chr_col: str | None = None,
    pos_col: str | None = None,
    gene_col: str | None = None,
    pvalue_col: str | None = None,
    rsid_col: str | None = None,
    sentinel_col: str | None = None,
) -> StudyInspectionResult:
    """Full inspection of sentinel data for study readiness."""
    columns = _profile_columns(df)
    detection = _detect_sentinel_columns(df)

    # Override detected columns with explicit args
    if chr_col:
        detection.chromosome = chr_col
    if pos_col:
        detection.position = pos_col
    if gene_col:
        detection.gene = gene_col
    if pvalue_col:
        detection.pvalue = pvalue_col
    if rsid_col:
        detection.rsid = rsid_col
    if sentinel_col:
        detection.sentinel_id = sentinel_col

    # Chromosome analysis
    chr_analysis = None
    if detection.chromosome and detection.chromosome in df.columns:
        chr_analysis = _analyze_chromosomes(df, detection.chromosome)

    # Position analysis
    pos_analysis = None
    if detection.position and detection.position in df.columns:
        pos_analysis = _analyze_positions(df, detection.position)

    # Gene analysis
    gene_analysis = None
    if detection.gene and detection.gene in df.columns:
        gene_analysis = _analyze_genes(df, detection.gene)

    # Trait analysis
    trait_analysis = None
    if detection.trait and detection.trait in df.columns:
        trait_analysis = _analyze_traits(df, detection.trait)

    # Clustering preview — requires chr + pos
    # Clusters per trait (matching study add behavior) when trait column is present
    clustering = None
    if detection.chromosome and detection.position:
        clustering = preview_clustering(
            df,
            chr_col=detection.chromosome,
            pos_col=detection.position,
            trait_col=detection.trait,
            window_kb=window_kb,
            merge_distance_kb=merge_distance_kb,
            cache_dir=cache_dir,
        )

    # Suggested fixes
    fixes = _generate_fixes(chr_analysis, pos_analysis, gene_analysis, detection)

    score = _compute_readiness_score(
        detection, chr_analysis, pos_analysis, gene_analysis, clustering,
    )

    return StudyInspectionResult(
        source_label=source_label,
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        column_detection=detection,
        chromosome_analysis=chr_analysis,
        position_analysis=pos_analysis,
        gene_analysis=gene_analysis,
        trait_analysis=trait_analysis,
        clustering_preview=clustering,
        suggested_fixes=fixes,
        readiness_score=score,
    )


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------


def _detect_sentinel_columns(df: pd.DataFrame) -> SentinelColumnDetection:
    """Auto-detect which columns map to sentinel fields."""
    col_lower = {c.lower(): c for c in df.columns}
    detection = SentinelColumnDetection()

    for role, hints in _SENTINEL_COLUMN_HINTS.items():
        for hint in hints:
            if hint in col_lower:
                setattr(detection, role, col_lower[hint])
                break

    return detection


def _analyze_positions(df: pd.DataFrame, pos_col: str) -> PositionAnalysis:
    """Profile the position column."""
    series = df[pos_col]
    total = len(series)
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    invalid_mask = series.notna() & numeric.isna()
    invalid_samples = [str(v) for v in series[invalid_mask].head(5).tolist()]

    return PositionAnalysis(
        total_rows=total,
        valid_count=len(valid),
        invalid_count=int(invalid_mask.sum()),
        invalid_samples=invalid_samples,
        min_position=int(valid.min()) if len(valid) > 0 else None,
        max_position=int(valid.max()) if len(valid) > 0 else None,
    )


def _analyze_traits(df: pd.DataFrame, trait_col: str) -> TraitAnalysis:
    """Analyze trait column."""
    series = df[trait_col].dropna().astype(str)
    counts = series.value_counts().to_dict()
    unique = list(counts.keys())

    return TraitAnalysis(
        column_name=trait_col,
        unique_traits=unique,
        counts=counts,
    )


def preview_clustering(
    df: pd.DataFrame,
    *,
    chr_col: str = "chromosome",
    pos_col: str = "position",
    trait_col: str | None = None,
    window_kb: int = DEFAULT_WINDOW_KB,
    merge_distance_kb: int = DEFAULT_MERGE_DISTANCE_KB,
    cache_dir: Path | None = None,
) -> ClusteringPreview:
    """Run sentinel clustering and return preview without touching the database.

    Normalizes columns the same way add_study() does so the preview matches.
    When trait_col is set, clusters per trait (matching study add behavior)
    and deduplicates loci across traits for the combined preview.
    """
    # Normalize — same logic as study_management.add_study()
    work = df.copy()
    work.columns = [c.strip().lower() for c in work.columns]
    if "chr" in work.columns and "chromosome" not in work.columns:
        work = work.rename(columns={"chr": "chromosome"})
    if "pos" in work.columns and "position" not in work.columns:
        work = work.rename(columns={"pos": "position"})

    # If the caller detected different column names, also map them
    chr_lower = chr_col.strip().lower()
    pos_lower = pos_col.strip().lower()
    if chr_lower not in ("chromosome", "chr") and chr_lower in work.columns:
        work = work.rename(columns={chr_lower: "chromosome"})
    if pos_lower not in ("position", "pos") and pos_lower in work.columns:
        work = work.rename(columns={pos_lower: "position"})

    # Normalize trait_col to lowercased column name
    trait_lower = None
    if trait_col:
        trait_lower = trait_col.strip().lower()
        if trait_lower not in work.columns:
            trait_lower = None

    if "chromosome" not in work.columns or "position" not in work.columns:
        return ClusteringPreview(
            n_sentinels=len(df), n_loci=0,
            window_kb=window_kb, merge_distance_kb=merge_distance_kb,
            loci=[], by_chromosome={},
        )

    work["chromosome"] = work["chromosome"].astype(str)
    work["position"] = pd.to_numeric(work["position"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["chromosome", "position"])

    n_sentinels = len(work)
    if n_sentinels == 0:
        return ClusteringPreview(
            n_sentinels=0, n_loci=0,
            window_kb=window_kb, merge_distance_kb=merge_distance_kb,
            loci=[], by_chromosome={},
        )

    # Cluster per trait (matching study add behavior) or globally
    by_trait: dict[str, int] | None = None
    if trait_lower and trait_lower in work.columns:
        all_raw_loci = []
        by_trait = {}
        for trait_val, trait_df in work.groupby(trait_lower):
            trait_name = str(trait_val)
            trait_loci = _cluster_sentinels(
                trait_df, window_kb=window_kb, merge_distance_kb=merge_distance_kb,
            )
            by_trait[trait_name] = len(trait_loci)
            all_raw_loci.extend(trait_loci)
        raw_loci = all_raw_loci
    else:
        raw_loci = _cluster_sentinels(work, window_kb=window_kb, merge_distance_kb=merge_distance_kb)

    locus_previews = []
    by_chromosome: dict[str, int] = {}
    for locus in raw_loci:
        chrom = locus["chromosome"]
        start = locus["start"]
        end = locus["end"]
        sentinels = locus["sentinels"]
        positions = sorted(int(s["position"]) for s in sentinels if pd.notna(s.get("position")))

        # Cytoband naming
        locus_name = _get_locus_name_safe(chrom, start, end, cache_dir)

        locus_previews.append(LocusPreview(
            chromosome=str(chrom),
            start=start,
            end=end,
            n_sentinels=len(sentinels),
            locus_name=locus_name,
            sample_positions=positions[:3],
        ))

        by_chromosome[str(chrom)] = by_chromosome.get(str(chrom), 0) + 1

    # Sort by chromosome then start position
    locus_previews.sort(key=lambda lp: (len(lp.chromosome), lp.chromosome, lp.start))

    return ClusteringPreview(
        n_sentinels=n_sentinels,
        n_loci=len(locus_previews),
        window_kb=window_kb,
        merge_distance_kb=merge_distance_kb,
        loci=locus_previews,
        by_chromosome=by_chromosome,
        by_trait=by_trait,
    )


def _get_locus_name_safe(
    chromosome: str, start: int, end: int, cache_dir: Path | None,
) -> str:
    """Get cytoband name, falling back gracefully if unavailable."""
    if cache_dir is not None:
        try:
            from pegasus_v2f.cytoband import get_cytoband_for_region
            return get_cytoband_for_region(chromosome, start, end, cache_dir)
        except Exception:
            pass
    return f"chr{chromosome}:{start}-{end}"


# ---------------------------------------------------------------------------
# Suggested fixes
# ---------------------------------------------------------------------------


def _generate_fixes(
    chr_analysis: ChromosomeAnalysis | None,
    pos_analysis: PositionAnalysis | None,
    gene_analysis: GeneAnalysis | None,
    detection: SentinelColumnDetection,
) -> list[SuggestedFix]:
    fixes: list[SuggestedFix] = []

    if chr_analysis and chr_analysis.has_chr_prefix is True:
        fixes.append(SuggestedFix(
            "strip_chr_prefix",
            f"Chromosome column '{chr_analysis.column_name}' has 'chr' prefix. "
            f"Strip to match bare format (1, 2, ..., X) used by most sources.",
            {"type": "strip_prefix", "column": chr_analysis.column_name, "prefix": "chr"},
        ))
    elif chr_analysis and chr_analysis.has_chr_prefix is None:
        fixes.append(SuggestedFix(
            "mixed_chr_prefix",
            f"Chromosome column '{chr_analysis.column_name}' has mixed formats.",
            None,
        ))

    if pos_analysis and pos_analysis.invalid_count > 0:
        fixes.append(SuggestedFix(
            "invalid_positions",
            f"{pos_analysis.invalid_count} rows have non-numeric positions "
            f"(samples: {', '.join(pos_analysis.invalid_samples[:3])}). "
            f"These will be dropped during study add.",
            None,
        ))

    if gene_analysis and gene_analysis.null_count > 0:
        fixes.append(SuggestedFix(
            "null_genes",
            f"{gene_analysis.null_count} rows have empty gene column.",
            None,
        ))

    return fixes


# ---------------------------------------------------------------------------
# Readiness score
# ---------------------------------------------------------------------------


def _compute_readiness_score(
    detection: SentinelColumnDetection,
    chr_analysis: ChromosomeAnalysis | None,
    pos_analysis: PositionAnalysis | None,
    gene_analysis: GeneAnalysis | None,
    clustering: ClusteringPreview | None,
) -> float:
    score = 0.0

    # Has chromosome column (0.20)
    if detection.chromosome:
        score += 0.20

    # Has position column (0.20)
    if detection.position:
        score += 0.20

    # Position validity > 95% (0.15)
    if pos_analysis and pos_analysis.valid_count > 0:
        validity = pos_analysis.valid_count / pos_analysis.total_rows
        if validity > 0.95:
            score += 0.15

    # Chromosome format consistent (0.10)
    if chr_analysis and chr_analysis.format_consistency != "mixed":
        score += 0.10

    # Has at least one sentinel field (gene, pvalue, rsid, variant_id) (0.15)
    sentinel_fields = [detection.gene, detection.pvalue, detection.rsid, detection.sentinel_id]
    if any(sentinel_fields):
        score += 0.15

    # Clustering produces reasonable loci (0.10)
    if clustering and clustering.n_loci > 0:
        score += 0.10

    # Gene validation > 90% if present (0.10)
    if gene_analysis and gene_analysis.valid_hgnc_pct > 90:
        score += 0.10

    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Report conversion and rendering
# ---------------------------------------------------------------------------


def study_inspection_to_report(result: StudyInspectionResult) -> Report:
    """Convert StudyInspectionResult to a Report."""
    report = Report(operation=f"study_inspect: {result.source_label}")
    report.counters["rows"] = result.row_count
    report.counters["columns"] = result.column_count

    det = result.column_detection
    detected = [f for f in ["chromosome", "position", "trait", "gene", "pvalue", "rsid"]
                if getattr(det, f)]
    report.info("detected_columns", f"Detected: {', '.join(detected)}")

    if result.position_analysis:
        pa = result.position_analysis
        report.info("positions", f"{pa.valid_count}/{pa.total_rows} valid positions")
        if pa.invalid_count:
            report.warning("invalid_positions", f"{pa.invalid_count} invalid positions")

    if result.trait_analysis:
        ta = result.trait_analysis
        report.info("traits", f"{len(ta.unique_traits)} traits: {', '.join(ta.unique_traits)}")

    if result.clustering_preview:
        cp = result.clustering_preview
        suffix = " (per trait)" if cp.by_trait else ""
        report.info("clustering", f"{cp.n_sentinels} sentinels -> {cp.n_loci} loci{suffix}")

    for fix in result.suggested_fixes:
        report.warning(fix.code, fix.message)

    return report


def render_study_inspection(
    result: StudyInspectionResult, console: Any = None,
) -> None:
    """Rich-render study inspection results to stderr."""
    from rich.console import Console
    from rich.table import Table

    if console is None:
        import sys
        console = Console(stderr=True, file=sys.stderr)

    # Header
    console.print(
        f"\n[bold]Sentinel Inspection: {result.source_label}[/bold] "
        f"({result.row_count:,} rows, {result.column_count} columns)\n"
    )

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

    # Column detection
    det = result.column_detection
    console.print("\n  [bold]Detected Columns[/bold]")
    mappings = []
    for role in ["chromosome", "position", "trait", "gene", "pvalue", "rsid", "sentinel_id"]:
        val = getattr(det, role)
        if val:
            mappings.append(f"{role} -> {val}")
    if mappings:
        console.print(f"    {', '.join(mappings)}")
    else:
        console.print("    [yellow]No sentinel columns detected[/yellow]")

    # Position analysis
    if result.position_analysis:
        pa = result.position_analysis
        console.print(f"\n  [bold]Position Validation[/bold]")
        console.print(
            f"    {pa.valid_count:,}/{pa.total_rows:,} "
            f"({pa.valid_count / pa.total_rows * 100:.0f}%) valid positions"
        )
        if pa.min_position is not None:
            console.print(f"    Range: {pa.min_position:,} - {pa.max_position:,}")
        if pa.invalid_count > 0:
            console.print(
                f"    [yellow]{pa.invalid_count} invalid "
                f"(samples: {', '.join(pa.invalid_samples[:3])})[/yellow]"
            )

    # Chromosome analysis
    if result.chromosome_analysis:
        ca = result.chromosome_analysis
        console.print(f"\n  [bold]Chromosome Format[/bold]")
        if ca.has_chr_prefix is True:
            console.print("    [yellow]All values have 'chr' prefix[/yellow]")
        elif ca.has_chr_prefix is None:
            console.print("    [yellow]Mixed formats (some with 'chr', some without)[/yellow]")
        else:
            console.print("    [green]Consistent bare format (no prefix)[/green]")

    # Gene analysis
    if result.gene_analysis:
        ga = result.gene_analysis
        non_null = ga.total_values - ga.null_count
        console.print(f"\n  [bold]Gene Validation[/bold] ({ga.column_name})")
        console.print(
            f"    {ga.valid_hgnc_pct:.1f}% valid HGNC format "
            f"({ga.unique_count} unique, {ga.null_count} null)"
        )
        if ga.sample_invalid:
            console.print(f"    [yellow]Sample invalid: {', '.join(ga.sample_invalid[:5])}[/yellow]")

    # Trait analysis
    if result.trait_analysis:
        ta = result.trait_analysis
        console.print(f"\n  [bold]Trait Distribution[/bold]")
        parts = [f"{t}: {c}" for t, c in ta.counts.items()]
        console.print(f"    {' | '.join(parts)}")

    # Clustering preview
    if result.clustering_preview:
        cp = result.clustering_preview
        console.print(
            f"\n  [bold]Clustering Preview[/bold] "
            f"(window: +/-{cp.window_kb}kb, merge: {cp.merge_distance_kb}kb)"
        )
        if cp.by_trait:
            console.print(f"    {cp.n_sentinels:,} sentinels -> {cp.n_loci:,} loci (clustered per trait)")
            trait_parts = [f"{t}: {n} loci" for t, n in cp.by_trait.items()]
            console.print(f"    {' | '.join(trait_parts)}")
        else:
            console.print(f"    {cp.n_sentinels:,} sentinels -> {cp.n_loci:,} loci")

        if cp.loci:
            locus_table = Table()
            locus_table.add_column("Chr", style="dim")
            locus_table.add_column("Range")
            locus_table.add_column("Signals", justify="right")
            locus_table.add_column("Cytoband", style="bold")

            display_loci = cp.loci[:15]
            for lp in display_loci:
                locus_table.add_row(
                    lp.chromosome,
                    f"{lp.start:,}-{lp.end:,}",
                    str(lp.n_sentinels),
                    lp.locus_name,
                )
            if len(cp.loci) > 15:
                locus_table.add_row("...", "...", "...", f"({len(cp.loci) - 15} more)")
            console.print(locus_table)

        # By chromosome summary
        if cp.by_chromosome:
            chr_parts = [f"{c}->{n}" for c, n in sorted(cp.by_chromosome.items(),
                         key=lambda x: (len(x[0]), x[0]))]
            console.print(f"    By chr: {', '.join(chr_parts)}")

    # Suggested fixes
    if result.suggested_fixes:
        console.print(f"\n  [bold]Suggested Fixes[/bold]")
        for i, fix in enumerate(result.suggested_fixes, 1):
            console.print(f"    {i}. [yellow]{fix.code}[/yellow]: {fix.message}")

    # Readiness score
    score = result.readiness_score
    style = "green" if score >= 0.7 else "yellow" if score >= 0.4 else "red"
    console.print(f"\n  [bold]Readiness:[/bold] [{style}]{score:.2f} / 1.0[/{style}]\n")
