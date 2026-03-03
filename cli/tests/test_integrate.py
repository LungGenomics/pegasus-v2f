"""Tests for integration wizard — detect, suggest, validate, apply."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.integrate import (
    detect_columns,
    suggest_mappings,
    validate_mapping,
    apply_integration,
    _update_yaml_evidence_block,
)


@pytest.fixture
def wizard_db():
    """DB with PEGASUS schema, a locus, and a raw table to integrate."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    # Study + locus (needed for scoring after integration)
    c.execute("INSERT INTO studies (study_id, trait) VALUES ('s1', 'HEIGHT')")
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1', '1', 900000, 1100000, 1e-10)"
    )

    # Raw table simulating a gene-level annotation
    c.execute(
        "CREATE TABLE secretome AS SELECT * FROM (VALUES "
        "('GENE_A', 'Secreted', 0.95), "
        "('GENE_B', 'Membrane', 0.80) "
        ") t(gene, location, confidence)"
    )

    yield c
    c.close()


class TestDetectColumns:
    def test_detects_columns(self, wizard_db):
        cols = detect_columns(wizard_db, "secretome")
        names = [c["name"] for c in cols]
        assert "gene" in names
        assert "location" in names
        assert "confidence" in names

    def test_includes_sample_values(self, wizard_db):
        cols = detect_columns(wizard_db, "secretome")
        gene_col = next(c for c in cols if c["name"] == "gene")
        assert "GENE_A" in gene_col["sample_values"]

    def test_infers_types(self, wizard_db):
        cols = detect_columns(wizard_db, "secretome")
        conf_col = next(c for c in cols if c["name"] == "confidence")
        assert conf_col["type"] == "numeric"


class TestSuggestMappings:
    def test_suggests_gene_field(self):
        columns = [{"name": "gene", "type": "text", "sample_values": ["BRCA1"]}]
        result = suggest_mappings(columns)
        assert result["fields"]["gene"] == "gene"

    def test_suggests_category_from_name(self):
        columns = [{"name": "gene", "type": "text", "sample_values": []}]
        result = suggest_mappings(columns, source_name="viktor_coloc")
        assert result["category"] == "COLOC"

    def test_suggests_variant_centric_with_position(self):
        columns = [
            {"name": "gene", "type": "text", "sample_values": []},
            {"name": "chr", "type": "text", "sample_values": []},
            {"name": "pos", "type": "numeric", "sample_values": []},
        ]
        result = suggest_mappings(columns)
        assert result["centric"] == "variant"
        assert result["fields"]["chromosome"] == "chr"
        assert result["fields"]["position"] == "pos"

    def test_defaults_gene_centric_without_position(self):
        columns = [{"name": "gene", "type": "text", "sample_values": []}]
        result = suggest_mappings(columns)
        assert result["centric"] == "gene"

    def test_suggests_pvalue_field(self):
        columns = [
            {"name": "gene", "type": "text", "sample_values": []},
            {"name": "pvalue", "type": "numeric", "sample_values": []},
        ]
        result = suggest_mappings(columns)
        assert result["fields"]["pvalue"] == "pvalue"


class TestValidateMapping:
    def test_valid_gene_centric(self):
        mapping = {
            "category": "KNOW",
            "centric": "gene",
            "source_tag": "hpa",
            "fields": {"gene": "gene"},
        }
        assert validate_mapping(mapping) == []

    def test_valid_variant_centric(self):
        mapping = {
            "category": "COLOC",
            "centric": "variant",
            "source_tag": "coloc1",
            "fields": {"gene": "gene", "chromosome": "chr", "position": "pos"},
        }
        assert validate_mapping(mapping) == []

    def test_missing_category(self):
        mapping = {"centric": "gene", "source_tag": "x", "fields": {"gene": "g"}}
        errors = validate_mapping(mapping)
        assert any("category" in e.lower() for e in errors)

    def test_invalid_category(self):
        mapping = {
            "category": "INVALID",
            "centric": "gene",
            "source_tag": "x",
            "fields": {"gene": "g"},
        }
        errors = validate_mapping(mapping)
        assert any("Unknown category" in e for e in errors)

    def test_missing_gene_field(self):
        mapping = {
            "category": "KNOW",
            "centric": "gene",
            "source_tag": "x",
            "fields": {},
        }
        errors = validate_mapping(mapping)
        assert any("gene" in e.lower() for e in errors)

    def test_variant_missing_position(self):
        mapping = {
            "category": "COLOC",
            "centric": "variant",
            "source_tag": "x",
            "fields": {"gene": "g", "chromosome": "chr"},
        }
        errors = validate_mapping(mapping)
        assert any("position" in e.lower() for e in errors)


class TestApplyIntegration:
    def test_loads_evidence_and_drops_raw(self, wizard_db):
        config = {
            "pegasus": {
                "study": {"id_prefix": "test", "traits": ["HEIGHT"]},
                "integration": {"method": "criteria_count_v1", "effector_threshold": 0.25, "criteria": []},
            },
            "data_sources": [{"name": "secretome", "source_type": "memory"}],
        }
        mapping = {
            "category": "KNOW",
            "centric": "gene",
            "source_tag": "hpa_secretome",
            "fields": {"gene": "gene"},
        }
        result = apply_integration(wizard_db, "secretome", mapping, config)

        # Evidence was loaded
        rows = wizard_db.execute(
            "SELECT * FROM gene_evidence WHERE source_tag = 'hpa_secretome'"
        ).fetchall()
        assert len(rows) == 2

        # Raw table was dropped
        tables = [r[0] for r in wizard_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()]
        assert "secretome" not in tables

        # Scoring ran (0 because gene_evidence alone doesn't create locus-gene candidates)
        assert result["scores_computed"] >= 0
        assert result["raw_table_dropped"] is True


class TestUpdateYaml:
    def test_inserts_evidence_block(self, tmp_path):
        config_file = tmp_path / "v2f.yaml"
        config_file.write_text(
            "version: 1\n"
            "data_sources:\n"
            "  - name: secretome\n"
            "    source_type: file\n"
            "    path: data/raw/secretome.tsv\n"
            "  - name: other_source\n"
            "    source_type: file\n"
        )

        evidence_block = {
            "category": "KNOW",
            "centric": "gene",
            "source_tag": "hpa",
            "fields": {"gene": "gene"},
        }
        _update_yaml_evidence_block(config_file, "secretome", evidence_block)

        text = config_file.read_text()
        assert "evidence:" in text
        assert "category: KNOW" in text
        assert "centric: gene" in text
        assert "source_tag: hpa" in text

        # Other source should be untouched
        assert "other_source" in text

    def test_preserves_existing_content(self, tmp_path):
        config_file = tmp_path / "v2f.yaml"
        config_file.write_text(
            "version: 1\n"
            "# Important comment\n"
            "data_sources:\n"
            "  - name: my_source\n"
            "    source_type: file\n"
        )

        evidence_block = {
            "category": "GWAS",
            "centric": "gene",
            "source_tag": "gwas1",
            "fields": {"gene": "gene_symbol"},
        }
        _update_yaml_evidence_block(config_file, "my_source", evidence_block)

        text = config_file.read_text()
        assert "# Important comment" in text
        assert "version: 1" in text
