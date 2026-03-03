"""Tests for evidence config validation."""

import pandas as pd
import pytest

from pegasus_v2f.evidence_config import (
    validate_evidence_config,
    validate_pegasus_config,
    resolve_evidence_mapping,
)


class TestValidatePegasusConfig:
    def test_no_pegasus_section(self):
        assert validate_pegasus_config({"version": 1}) == []

    def test_missing_study(self):
        errors = validate_pegasus_config({"pegasus": {}})
        assert any("pegasus.study is required" in e for e in errors)

    def test_missing_id_prefix(self):
        config = {
            "pegasus": {"study": [{"traits": ["HEIGHT"]}]},
            "data_sources": [
                {"name": "s", "evidence": {"role": "locus_definition", "source_tag": "x",
                 "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert any("id_prefix" in e for e in errors)

    def test_missing_traits(self):
        config = {
            "pegasus": {"study": [{"id_prefix": "test"}]},
            "data_sources": [
                {"name": "s", "evidence": {"role": "locus_definition", "source_tag": "x",
                 "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert any("traits" in e for e in errors)

    def test_empty_traits(self):
        config = {
            "pegasus": {"study": [{"id_prefix": "test", "traits": []}]},
            "data_sources": [
                {"name": "s", "evidence": {"role": "locus_definition", "source_tag": "x",
                 "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert any("non-empty list" in e for e in errors)


    def test_no_locus_source(self):
        config = {
            "pegasus": {"study": [{"id_prefix": "test", "traits": ["HEIGHT"]}]},
            "data_sources": [
                {"name": "s", "evidence": {"centric": "gene", "category": "KNOW",
                 "source_tag": "x", "fields": {"gene": "g"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert any("locus source" in e for e in errors)

    def test_valid_with_locus_definition(self):
        config = {
            "pegasus": {"study": [{"id_prefix": "test", "traits": ["HEIGHT"]}]},
            "data_sources": [
                {"name": "s", "evidence": {"role": "locus_definition", "source_tag": "x",
                 "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert errors == []

    def test_valid_with_gwas_sumstats_only(self):
        config = {
            "pegasus": {"study": [{"id_prefix": "test", "traits": ["HEIGHT"]}]},
            "data_sources": [
                {"name": "ss", "evidence": {"role": "gwas_sumstats", "source_tag": "x",
                 "trait": "HEIGHT",
                 "fields": {"chromosome": "c", "position": "p", "pvalue": "pv"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert errors == []

    def test_invalid_integration_method(self):
        config = {
            "pegasus": {
                "study": [{"id_prefix": "test", "traits": ["HEIGHT"]}],
                "integration": {"method": "unknown_method"},
            },
            "data_sources": [
                {"name": "s", "evidence": {"role": "locus_definition", "source_tag": "x",
                 "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}}}
            ],
        }
        errors = validate_pegasus_config(config)
        assert any("not recognized" in e for e in errors)


class TestValidateEvidenceConfig:
    def test_no_evidence_block(self):
        assert validate_evidence_config({"name": "raw_source"}) == []

    def test_invalid_role(self):
        source = {"name": "s", "evidence": {"role": "invalid_role"}}
        errors = validate_evidence_config(source)
        assert any("not valid" in e for e in errors)

    def test_role_missing_source_tag(self):
        source = {
            "name": "s",
            "evidence": {"role": "locus_definition",
                         "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"}},
        }
        errors = validate_evidence_config(source)
        assert any("source_tag" in e for e in errors)

    def test_role_missing_fields(self):
        source = {
            "name": "s",
            "evidence": {"role": "locus_definition", "source_tag": "x", "fields": {"gene": "g"}},
        }
        errors = validate_evidence_config(source)
        assert any("missing required mappings" in e for e in errors)

    def test_valid_locus_definition(self):
        source = {
            "name": "s",
            "evidence": {
                "role": "locus_definition",
                "source_tag": "x",
                "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"},
            },
        }
        assert validate_evidence_config(source) == []

    def test_valid_gwas_sumstats(self):
        source = {
            "name": "ss",
            "evidence": {
                "role": "gwas_sumstats",
                "source_tag": "x",
                "pvalue_threshold": 5e-8,
                "fields": {"chromosome": "c", "position": "p", "pvalue": "pv"},
            },
        }
        assert validate_evidence_config(source) == []

    def test_invalid_pvalue_threshold(self):
        source = {
            "name": "ss",
            "evidence": {
                "role": "gwas_sumstats",
                "source_tag": "x",
                "pvalue_threshold": "not_a_number",
                "fields": {"chromosome": "c", "position": "p", "pvalue": "pv"},
            },
        }
        errors = validate_evidence_config(source)
        assert any("pvalue_threshold" in e for e in errors)

    def test_invalid_centric(self):
        source = {"name": "s", "evidence": {"centric": "invalid", "category": "QTL", "source_tag": "x"}}
        errors = validate_evidence_config(source)
        assert any("not valid" in e for e in errors)

    def test_centric_missing_category(self):
        source = {"name": "s", "evidence": {"centric": "gene", "source_tag": "x", "fields": {"gene": "g"}}}
        errors = validate_evidence_config(source)
        assert any("category is required" in e for e in errors)

    def test_invalid_category(self):
        source = {
            "name": "s",
            "evidence": {"centric": "gene", "category": "INVALID_CAT", "source_tag": "x",
                         "fields": {"gene": "g"}},
        }
        errors = validate_evidence_config(source)
        assert any("not a valid PEGASUS category" in e for e in errors)

    def test_neither_role_nor_centric(self):
        source = {"name": "s", "evidence": {"source_tag": "x"}}
        errors = validate_evidence_config(source)
        assert any("role" in e and "centric" in e for e in errors)

    def test_valid_gene_centric(self):
        source = {
            "name": "s",
            "evidence": {"centric": "gene", "category": "KNOW", "source_tag": "x",
                         "evidence_type": "secretome", "fields": {"gene": "g"}},
        }
        assert validate_evidence_config(source) == []

    def test_valid_variant_centric(self):
        source = {
            "name": "s",
            "evidence": {"centric": "variant", "category": "COLOC", "source_tag": "x",
                         "fields": {"gene": "g"}},
        }
        assert validate_evidence_config(source) == []


class TestResolveEvidenceMapping:
    def test_resolves_fields(self):
        source = {
            "name": "s",
            "evidence": {"fields": {"gene": "gene_name", "pvalue": "p_val"}},
        }
        df = pd.DataFrame({"gene_name": ["A"], "p_val": [0.01], "extra": [1]})
        mapping = resolve_evidence_mapping(source, df)
        assert mapping == {"gene": "gene_name", "pvalue": "p_val"}

    def test_missing_column_raises(self):
        source = {
            "name": "s",
            "evidence": {"fields": {"gene": "gene_name", "pvalue": "missing_col"}},
        }
        df = pd.DataFrame({"gene_name": ["A"], "other": [1]})
        with pytest.raises(ValueError, match="missing_col"):
            resolve_evidence_mapping(source, df)

    def test_no_evidence_block(self):
        source = {"name": "s"}
        df = pd.DataFrame({"a": [1]})
        assert resolve_evidence_mapping(source, df) == {}


class TestConfigIntegration:
    """Test that validate_config calls evidence validation."""

    def test_validate_config_catches_evidence_errors(self):
        from pegasus_v2f.config import validate_config

        config = {
            "version": 1,
            "pegasus": {"study": [{"id_prefix": "test", "traits": ["H"]}]},
            "data_sources": [
                {"name": "s", "source_type": "file",
                 "evidence": {"role": "invalid_role"}},
            ],
        }
        errors = validate_config(config)
        assert any("not valid" in e for e in errors)

    def test_validate_config_no_pegasus_skips_evidence(self):
        from pegasus_v2f.config import validate_config

        config = {
            "version": 1,
            "data_sources": [
                {"name": "s", "source_type": "file"},
            ],
        }
        errors = validate_config(config)
        assert errors == []
