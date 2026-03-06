"""Tests for source data inspection."""

import pandas as pd
import pytest

from pegasus_v2f.inspect import (
    InspectionResult,
    inspect_dataframe,
    inspection_to_report,
)


@pytest.fixture
def clean_df():
    """Clean gene-level data with no issues."""
    return pd.DataFrame({
        "gene": ["AGER", "SFTPC", "MUC5B", "TP53", "BRCA1"],
        "score": [0.95, 0.87, 0.42, 0.91, 0.33],
    })


@pytest.fixture
def variant_df():
    """Variant-level data with chr prefix."""
    return pd.DataFrame({
        "gene": ["AGER", "SFTPC", "MUC5B", "TP53", None],
        "chr": ["chr6", "chr8", "chr11", "chr17", "chr1"],
        "pos": [32151443, 22018712, 1219991, 7687490, 100000],
        "PP_H4_abf": [0.95, 0.87, 0.42, 0.91, 0.33],
    })


@pytest.fixture
def messy_df():
    """Data with mixed issues."""
    return pd.DataFrame({
        "gene": ["AGER", "sftpc", "", "TP53", None],
        "chr": ["chr6", "8", "chr11", "17", "chr1"],
        "pvalue": [0.001, 0.05, "NA", 0.0001, 0.5],
    })


class TestInspectCleanData:
    def test_high_compatibility(self, clean_df):
        result = inspect_dataframe(clean_df, source_name="test_source")
        assert result.compatibility_score >= 0.4
        assert result.row_count == 5
        assert result.column_count == 2

    def test_gene_analysis(self, clean_df):
        result = inspect_dataframe(clean_df, source_name="test")
        assert result.gene_analysis is not None
        assert result.gene_analysis.valid_hgnc_pct == 100.0
        assert result.gene_analysis.null_count == 0

    def test_no_fixes_needed(self, clean_df):
        result = inspect_dataframe(clean_df, source_name="test")
        # Clean data should have no fix suggestions
        assert len(result.suggested_fixes) == 0


class TestInspectVariantData:
    def test_chr_prefix_detected(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc_test")
        assert result.chromosome_analysis is not None
        assert result.chromosome_analysis.has_chr_prefix is True
        assert result.chromosome_analysis.format_consistency == "consistent_chr"

    def test_strip_chr_fix_suggested(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc_test")
        fix_codes = [f.code for f in result.suggested_fixes]
        assert "strip_chr_prefix" in fix_codes
        strip_fix = next(f for f in result.suggested_fixes if f.code == "strip_chr_prefix")
        assert strip_fix.transformation is not None
        assert strip_fix.transformation["type"] == "strip_prefix"

    def test_null_gene_fix_suggested(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc_test")
        fix_codes = [f.code for f in result.suggested_fixes]
        assert "drop_null_genes" in fix_codes

    def test_variant_centric_detected(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc_test")
        assert result.suggested_mappings["centric"] == "variant"

    def test_coloc_category_from_name(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc_test")
        assert result.suggested_mappings["category"] == "COLOC"


class TestInspectMessyData:
    def test_mixed_chr_format(self, messy_df):
        result = inspect_dataframe(messy_df, source_name="test")
        assert result.chromosome_analysis is not None
        assert result.chromosome_analysis.has_chr_prefix is None
        assert result.chromosome_analysis.format_consistency == "mixed"

    def test_mixed_case_gene_fix(self, messy_df):
        result = inspect_dataframe(messy_df, source_name="test")
        fix_codes = [f.code for f in result.suggested_fixes]
        assert "normalize_gene_case" in fix_codes

    def test_coerce_numeric_fix(self, messy_df):
        result = inspect_dataframe(messy_df, source_name="test")
        fix_codes = [f.code for f in result.suggested_fixes]
        assert "coerce_numeric" in fix_codes

    def test_lower_compatibility_score(self, messy_df, clean_df):
        messy_result = inspect_dataframe(messy_df, source_name="test")
        clean_result = inspect_dataframe(clean_df, source_name="test")
        assert messy_result.compatibility_score < clean_result.compatibility_score


class TestInspectCategoryDetection:
    def test_category_from_source_name(self, clean_df):
        for name, expected in [("my_eqtl_data", "QTL"), ("gwas_results", "GWAS"),
                               ("deg_analysis", "EXP"), ("secretome_data", "KNOW")]:
            result = inspect_dataframe(clean_df, source_name=name)
            assert result.suggested_mappings["category"] == expected, f"Failed for {name}"


class TestSerialization:
    def test_to_dict(self, clean_df):
        result = inspect_dataframe(clean_df, source_name="test")
        d = result.to_dict()
        assert d["source_name"] == "test"
        assert d["row_count"] == 5
        assert isinstance(d["columns"], list)

    def test_to_json(self, clean_df):
        result = inspect_dataframe(clean_df, source_name="test")
        j = result.to_json()
        import json
        parsed = json.loads(j)
        assert parsed["source_name"] == "test"

    def test_to_report(self, variant_df):
        result = inspect_dataframe(variant_df, source_name="coloc")
        report = inspection_to_report(result)
        assert report.operation == "inspect: coloc"
        assert report.has_warnings  # chr prefix warning
