"""Tests for transform.py — column cleaning and transformations."""

import pandas as pd
import pytest

from pegasus_v2f.transform import clean_for_db, apply_transformations


class TestCleanForDb:
    def test_periods_to_underscores(self):
        df = pd.DataFrame({"col.name": [1], "another.col.here": [2]})
        result = clean_for_db(df)
        assert "col_name" in result.columns
        assert "another_col_here" in result.columns

    def test_spaces_to_underscores(self):
        df = pd.DataFrame({"col name": [1], "multiple  spaces": [2]})
        result = clean_for_db(df)
        assert "col_name" in result.columns
        assert "multiple_spaces" in result.columns

    def test_special_chars_removed(self):
        df = pd.DataFrame({"col(1)": [1], "col-2": [2], "col#3": [3]})
        result = clean_for_db(df)
        assert "col_1" in result.columns
        assert "col_2" in result.columns
        assert "col_3" in result.columns

    def test_drops_unnamed_columns(self):
        df = pd.DataFrame({"gene": [1], "...1": [2], "Unnamed: 0": [3]})
        result = clean_for_db(df)
        assert "gene" in result.columns
        assert len(result.columns) == 1

    def test_collapse_consecutive_underscores(self):
        df = pd.DataFrame({"a__b___c": [1]})
        result = clean_for_db(df)
        assert "a_b_c" in result.columns

    def test_strip_leading_trailing_underscores(self):
        df = pd.DataFrame({"_leading": [1], "trailing_": [2]})
        result = clean_for_db(df)
        assert "leading" in result.columns
        assert "trailing" in result.columns

    def test_bool_to_str(self):
        df = pd.DataFrame({"flag": [True, False]})
        result = clean_for_db(df)
        # Should no longer be bool — converted to string type
        assert result["flag"].dtype != "bool"
        assert str(result["flag"].iloc[0]) == "True"


class TestApplyTransformations:
    def test_rename(self):
        df = pd.DataFrame({"old_name": [1, 2]})
        result = apply_transformations(df, [
            {"type": "rename", "columns": {"old_name": "new_name"}}
        ])
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_rename_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        result = apply_transformations(df, [
            {"type": "rename", "columns": {"nonexistent": "new"}}
        ])
        assert list(result.columns) == ["a"]

    def test_select(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = apply_transformations(df, [
            {"type": "select", "columns": ["a", "c"]}
        ])
        assert list(result.columns) == ["a", "c"]

    def test_deduplicate(self):
        df = pd.DataFrame({"gene": ["A", "A", "B"], "val": [1, 2, 3]})
        result = apply_transformations(df, [
            {"type": "deduplicate", "column": "gene"}
        ])
        assert len(result) == 2
        assert result.iloc[0]["val"] == 1  # keeps first

    def test_parse_variant_id_chr_prefix(self):
        df = pd.DataFrame({
            "PrimaryVariantId": [
                "chr1:16979534C:A",
                "chr4:15579131T:G",
                "chr10:103897116G:A",
            ],
            "pvalue": [1e-10, 1e-8, 1e-12],
        })
        result = apply_transformations(df, [
            {"type": "parse_variant_id", "column": "PrimaryVariantId"}
        ])
        assert "chr" in result.columns
        assert "pos" in result.columns
        assert list(result["chr"]) == ["1", "4", "10"]
        assert list(result["pos"]) == [16979534, 15579131, 103897116]

    def test_parse_variant_id_no_prefix(self):
        df = pd.DataFrame({
            "variant": ["1:16979534:C:A", "10:103897116:G:A"],
        })
        result = apply_transformations(df, [
            {"type": "parse_variant_id", "column": "variant"}
        ])
        assert list(result["chr"]) == ["1", "10"]
        assert list(result["pos"]) == [16979534, 103897116]

    def test_parse_variant_id_pos_only(self):
        df = pd.DataFrame({
            "vid": ["chr3:44861942", "chr6:7562999"],
        })
        result = apply_transformations(df, [
            {"type": "parse_variant_id", "column": "vid"}
        ])
        assert list(result["chr"]) == ["3", "6"]
        assert list(result["pos"]) == [44861942, 7562999]

    def test_parse_variant_id_case_insensitive(self):
        """Column names may be lowercased before transforms run."""
        df = pd.DataFrame({
            "primaryvariantid": ["chr1:16979534C:A", "chr10:103897116G:A"],
        })
        result = apply_transformations(df, [
            {"type": "parse_variant_id", "column": "PrimaryVariantId"}
        ])
        assert list(result["chr"]) == ["1", "10"]
        assert list(result["pos"]) == [16979534, 103897116]

    def test_parse_variant_id_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        result = apply_transformations(df, [
            {"type": "parse_variant_id", "column": "nonexistent"}
        ])
        assert "chr" not in result.columns

    def test_split_column_default(self):
        df = pd.DataFrame({"name": ["GATA5_rs200383755_C_G", "TP53_rs123_A_T"]})
        result = apply_transformations(df, [
            {"type": "split_column", "column": "name", "delimiter": "_", "index": 0}
        ])
        assert list(result["name"]) == ["GATA5", "TP53"]

    def test_split_column_output(self):
        df = pd.DataFrame({"combo": ["chr1:12345", "chr2:67890"]})
        result = apply_transformations(df, [
            {"type": "split_column", "column": "combo", "delimiter": ":", "index": 1, "output": "pos"}
        ])
        assert list(result["pos"]) == ["12345", "67890"]
        assert list(result["combo"]) == ["chr1:12345", "chr2:67890"]  # original preserved

    def test_split_column_missing(self):
        df = pd.DataFrame({"a": [1]})
        result = apply_transformations(df, [
            {"type": "split_column", "column": "nonexistent"}
        ])
        assert list(result.columns) == ["a"]

    def test_unknown_type_warns(self):
        df = pd.DataFrame({"a": [1]})
        result = apply_transformations(df, [{"type": "bogus"}])
        assert len(result) == 1  # unchanged
