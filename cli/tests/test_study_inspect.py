"""Tests for sentinel data inspection — column detection, position validation, clustering preview."""

import json

import pandas as pd
import pytest

from pegasus_v2f.study_inspect import (
    ClusteringPreview,
    LocusPreview,
    PositionAnalysis,
    SentinelColumnDetection,
    StudyInspectionResult,
    TraitAnalysis,
    _analyze_positions,
    _analyze_traits,
    _detect_sentinel_columns,
    inspect_sentinels,
    preview_clustering,
    study_inspection_to_report,
)


@pytest.fixture
def clean_sentinel_df():
    """Clean sentinel data with 3 variants on 2 chromosomes."""
    return pd.DataFrame({
        "chromosome": ["1", "1", "2"],
        "position": [1_000_000, 1_200_000, 5_000_000],
        "rsid": ["rs1", "rs2", "rs3"],
        "pvalue": [1e-10, 1e-8, 1e-12],
        "gene": ["AGER", "SFTPC", "MUC5B"],
    })


@pytest.fixture
def messy_sentinel_df():
    """Sentinel data with common problems."""
    return pd.DataFrame({
        "chr": ["chr1", "1", "chr2", "chr3"],
        "pos": [1_000_000, "invalid", 5_000_000, 8_000_000],
        "rsid": ["rs1", "rs2", "rs3", "rs4"],
        "pvalue": [1e-10, 1e-8, "NA", 1e-6],
        "gene": ["AGER", "sftpc", None, "MUC5B"],
    })


@pytest.fixture
def multi_trait_df():
    """Sentinel data with trait column."""
    return pd.DataFrame({
        "chromosome": ["1", "1", "2", "2"],
        "position": [1_000_000, 5_000_000, 3_000_000, 7_000_000],
        "trait": ["FEV1", "FEV1", "FVC", "FVC"],
        "pvalue": [1e-10, 1e-8, 1e-12, 1e-6],
    })


class TestColumnDetection:
    def test_detects_standard_columns(self, clean_sentinel_df):
        det = _detect_sentinel_columns(clean_sentinel_df)
        assert det.chromosome == "chromosome"
        assert det.position == "position"
        assert det.gene == "gene"
        assert det.pvalue == "pvalue"
        assert det.rsid == "rsid"

    def test_detects_chr_pos_aliases(self, messy_sentinel_df):
        det = _detect_sentinel_columns(messy_sentinel_df)
        assert det.chromosome == "chr"
        assert det.position == "pos"

    def test_detects_trait_column(self, multi_trait_df):
        det = _detect_sentinel_columns(multi_trait_df)
        assert det.trait == "trait"

    def test_no_sentinel_id_when_absent(self, clean_sentinel_df):
        det = _detect_sentinel_columns(clean_sentinel_df)
        assert det.sentinel_id is None

    def test_serialization(self, clean_sentinel_df):
        det = _detect_sentinel_columns(clean_sentinel_df)
        d = det.to_dict()
        assert d["chromosome"] == "chromosome"
        assert d["sentinel_id"] is None


class TestPositionAnalysis:
    def test_all_valid_positions(self, clean_sentinel_df):
        pa = _analyze_positions(clean_sentinel_df, "position")
        assert pa.valid_count == 3
        assert pa.invalid_count == 0
        assert pa.invalid_samples == []
        assert pa.min_position == 1_000_000
        assert pa.max_position == 5_000_000

    def test_invalid_positions_detected(self, messy_sentinel_df):
        pa = _analyze_positions(messy_sentinel_df, "pos")
        assert pa.invalid_count == 1
        assert "invalid" in pa.invalid_samples

    def test_position_range(self, clean_sentinel_df):
        pa = _analyze_positions(clean_sentinel_df, "position")
        assert pa.min_position < pa.max_position

    def test_serialization(self, clean_sentinel_df):
        pa = _analyze_positions(clean_sentinel_df, "position")
        d = pa.to_dict()
        assert d["valid_count"] == 3
        assert d["min_position"] == 1_000_000


class TestTraitAnalysis:
    def test_trait_detection(self, multi_trait_df):
        ta = _analyze_traits(multi_trait_df, "trait")
        assert set(ta.unique_traits) == {"FEV1", "FVC"}

    def test_trait_counts(self, multi_trait_df):
        ta = _analyze_traits(multi_trait_df, "trait")
        assert ta.counts["FEV1"] == 2
        assert ta.counts["FVC"] == 2

    def test_serialization(self, multi_trait_df):
        ta = _analyze_traits(multi_trait_df, "trait")
        d = ta.to_dict()
        assert "FEV1" in d["unique_traits"]
        assert d["counts"]["FEV1"] == 2


class TestClusteringPreview:
    def test_nearby_sentinels_merge(self, clean_sentinel_df):
        """Two sentinels 200kb apart with 500kb window should merge."""
        cp = preview_clustering(
            clean_sentinel_df,
            window_kb=500,
            merge_distance_kb=250,
        )
        # chr1 sentinels at 1M and 1.2M are within window → 1 locus
        # chr2 sentinel at 5M → 1 locus
        assert cp.n_loci == 2
        assert cp.n_sentinels == 3

    def test_small_window_prevents_merge(self, clean_sentinel_df):
        """With tiny window, each sentinel gets its own locus."""
        cp = preview_clustering(
            clean_sentinel_df,
            window_kb=50,
            merge_distance_kb=0,
        )
        assert cp.n_loci == 3

    def test_by_chromosome(self, clean_sentinel_df):
        cp = preview_clustering(clean_sentinel_df, window_kb=500, merge_distance_kb=250)
        assert "1" in cp.by_chromosome
        assert "2" in cp.by_chromosome

    def test_locus_preview_fields(self, clean_sentinel_df):
        cp = preview_clustering(clean_sentinel_df, window_kb=500, merge_distance_kb=250)
        assert all(isinstance(lp, LocusPreview) for lp in cp.loci)
        for lp in cp.loci:
            assert lp.start < lp.end
            assert lp.n_sentinels >= 1

    def test_fallback_names_without_cache(self, clean_sentinel_df):
        """Without cache_dir, locus names use chr:pos format."""
        cp = preview_clustering(clean_sentinel_df, window_kb=500, merge_distance_kb=250)
        for lp in cp.loci:
            assert "chr" in lp.locus_name

    def test_handles_chr_pos_aliases(self, messy_sentinel_df):
        """Should work with 'chr'/'pos' column names."""
        cp = preview_clustering(
            messy_sentinel_df,
            chr_col="chr",
            pos_col="pos",
            window_kb=500,
            merge_distance_kb=250,
        )
        # 1 invalid position row dropped, 3 valid remain
        assert cp.n_sentinels == 3

    def test_per_trait_clustering(self, multi_trait_df):
        """With trait column, clusters per trait (matching study add behavior)."""
        cp = preview_clustering(
            multi_trait_df,
            trait_col="trait",
            window_kb=500,
            merge_distance_kb=250,
        )
        assert cp.by_trait is not None
        assert "FEV1" in cp.by_trait
        assert "FVC" in cp.by_trait
        # Per-trait loci can be more than global clustering would produce
        # because sentinels that would merge globally stay separate per trait
        assert cp.n_loci == sum(cp.by_trait.values())

    def test_no_trait_clustering_is_global(self, clean_sentinel_df):
        """Without trait column, by_trait is None."""
        cp = preview_clustering(clean_sentinel_df, window_kb=500, merge_distance_kb=250)
        assert cp.by_trait is None

    def test_empty_result_without_columns(self):
        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        cp = preview_clustering(df, window_kb=500, merge_distance_kb=250)
        assert cp.n_loci == 0

    def test_serialization(self, clean_sentinel_df):
        cp = preview_clustering(clean_sentinel_df, window_kb=500, merge_distance_kb=250)
        d = cp.to_dict()
        assert d["n_sentinels"] == 3
        assert len(d["loci"]) == cp.n_loci


class TestReadinessScore:
    def test_clean_data_high_score(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        assert result.readiness_score >= 0.8

    def test_messy_data_lower_score(self, messy_sentinel_df):
        result = inspect_sentinels(messy_sentinel_df, "test")
        assert result.readiness_score < 0.9

    def test_no_position_column_low_score(self):
        df = pd.DataFrame({"gene": ["AGER", "SFTPC"], "value": [1, 2]})
        result = inspect_sentinels(df, "test")
        assert result.readiness_score < 0.5

    def test_no_chromosome_column_lower_score(self):
        df = pd.DataFrame({"position": [1000, 2000], "gene": ["A", "B"]})
        result = inspect_sentinels(df, "test")
        # Has position + gene but no chromosome → no clustering possible
        assert result.readiness_score < 0.7
        assert result.clustering_preview is None


class TestSuggestedFixes:
    def test_strip_chr_prefix(self):
        df = pd.DataFrame({
            "chromosome": ["chr1", "chr2", "chr3"],
            "position": [1_000_000, 2_000_000, 3_000_000],
        })
        result = inspect_sentinels(df, "test")
        codes = [f.code for f in result.suggested_fixes]
        assert "strip_chr_prefix" in codes

    def test_invalid_positions_fix(self, messy_sentinel_df):
        result = inspect_sentinels(messy_sentinel_df, "test")
        codes = [f.code for f in result.suggested_fixes]
        assert "invalid_positions" in codes

    def test_mixed_chr_format(self, messy_sentinel_df):
        result = inspect_sentinels(messy_sentinel_df, "test")
        codes = [f.code for f in result.suggested_fixes]
        assert "mixed_chr_prefix" in codes

    def test_clean_data_no_fixes(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        assert len(result.suggested_fixes) == 0


class TestInspectSentinels:
    def test_full_result_structure(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test_source")
        assert result.source_label == "test_source"
        assert result.row_count == 3
        assert result.column_count == 5
        assert len(result.columns) == 5
        assert result.column_detection.chromosome == "chromosome"
        assert result.position_analysis is not None
        assert result.clustering_preview is not None
        assert result.gene_analysis is not None

    def test_explicit_gene_col_override(self):
        df = pd.DataFrame({
            "chromosome": ["1"],
            "position": [1000000],
            "nearest_gene": ["AGER"],
            "other_gene": ["TP53"],
        })
        result = inspect_sentinels(df, "test", gene_col="other_gene")
        assert result.gene_analysis is not None
        assert result.gene_analysis.column_name == "other_gene"

    def test_no_trait_when_absent(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        assert result.trait_analysis is None

    def test_trait_detected(self, multi_trait_df):
        result = inspect_sentinels(multi_trait_df, "test")
        assert result.trait_analysis is not None
        assert len(result.trait_analysis.unique_traits) == 2


class TestSerialization:
    def test_to_dict(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        d = result.to_dict()
        assert d["source_label"] == "test"
        assert d["row_count"] == 3
        assert "columns" in d
        assert "column_detection" in d
        assert "readiness_score" in d

    def test_to_json(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        j = result.to_json()
        parsed = json.loads(j)
        assert parsed["row_count"] == 3

    def test_to_report(self, clean_sentinel_df):
        result = inspect_sentinels(clean_sentinel_df, "test")
        report = study_inspection_to_report(result)
        assert report.operation == "study_inspect: test"
        assert report.counters["rows"] == 3
