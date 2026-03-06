"""Tests for PEGASUS export — evidence matrix, metadata, PEG list."""

import duckdb
import pytest
import yaml

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.pegasus_export import (
    export_evidence_matrix,
    export_metadata,
    export_peg_list,
    export_all,
)


@pytest.fixture
def export_db():
    """DB with synthetic data ready for export."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    c.execute(
        "INSERT INTO studies (study_id, study_name, trait, gwas_source) "
        "VALUES ('s1', 'test', 'HEIGHT', 'PMID:1')"
    )
    c.execute(
        "INSERT INTO loci (locus_id, study_id, locus_name, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1', 'GENE_A', '1', 900000, 1100000, 1e-10), "
        "       ('l2', 's1', 'GENE_C', '2', 5000000, 6000000, 1e-12)"
    )
    # Scored evidence
    c.execute(
        "INSERT INTO scored_evidence (locus_id, study_id, gene_symbol, evidence_category, "
        "source_tag, pvalue, score, match_type, integration_rank, is_predicted_effector, n_candidate_genes) "
        "VALUES ('l1', 's1', 'GENE_A', 'GWAS', 'src1', 1e-10, NULL, 'gene', 1, TRUE, 2), "
        "       ('l1', 's1', 'GENE_A', 'COLOC', 'src2', NULL, 0.95, 'gene', 1, TRUE, 2), "
        "       ('l1', 's1', 'GENE_B', 'GWAS', 'src1', 1e-5, NULL, 'gene', 2, FALSE, 2), "
        "       ('l2', 's1', 'GENE_C', 'GWAS', 'src1', 1e-12, NULL, 'gene', 1, TRUE, 1)"
    )
    # Data sources
    c.execute(
        "INSERT INTO data_sources (source_tag, source_name, evidence_category, is_integrated) "
        "VALUES ('src1', 'GWAS Source', 'GWAS', TRUE), ('src2', 'Coloc Source', 'COLOC', TRUE)"
    )

    yield c
    c.close()


class TestExportEvidenceMatrix:
    def test_creates_tsv(self, export_db, tmp_path):
        path = export_evidence_matrix(export_db, ["s1"], tmp_path)
        assert path.exists()
        assert path.suffix == ".tsv"

    def test_has_correct_columns(self, export_db, tmp_path):
        path = export_evidence_matrix(export_db, ["s1"], tmp_path)
        with open(path) as f:
            header = f.readline().strip().split("\t")
        assert "locus_id" in header
        assert "gene_symbol" in header
        assert "GWAS" in header
        assert "COLOC" in header

    def test_row_count(self, export_db, tmp_path):
        path = export_evidence_matrix(export_db, ["s1"], tmp_path)
        with open(path) as f:
            lines = f.readlines()
        # header + 3 gene-locus pairs (GENE_A@l1, GENE_B@l1, GENE_C@l2)
        assert len(lines) == 4

    def test_empty_study(self, export_db, tmp_path):
        path = export_evidence_matrix(export_db, ["nonexistent"], tmp_path)
        assert path.exists()


class TestExportMetadata:
    def test_creates_yaml(self, export_db, tmp_path):
        path = export_metadata(export_db, ["s1"], tmp_path)
        assert path.exists()
        meta = yaml.safe_load(path.read_text())
        assert "HEIGHT" in meta["study"]["traits"]
        assert meta["n_loci"] == 2
        assert "GWAS" in meta["evidence_categories"]

    def test_includes_data_sources(self, export_db, tmp_path):
        path = export_metadata(export_db, ["s1"], tmp_path)
        meta = yaml.safe_load(path.read_text())
        assert len(meta["data_sources"]) == 2


class TestExportPegList:
    def test_creates_tsv(self, export_db, tmp_path):
        path = export_peg_list(export_db, ["s1"], tmp_path)
        assert path.exists()

    def test_one_gene_per_locus(self, export_db, tmp_path):
        path = export_peg_list(export_db, ["s1"], tmp_path)
        with open(path) as f:
            lines = f.readlines()
        # header + 2 loci (rank 1 per locus)
        assert len(lines) == 3

    def test_correct_top_genes(self, export_db, tmp_path):
        path = export_peg_list(export_db, ["s1"], tmp_path)
        with open(path) as f:
            lines = f.readlines()
        content = "\t".join(lines[1:])
        assert "GENE_A" in content
        assert "GENE_C" in content


class TestExportAll:
    def test_exports_three_files(self, export_db, tmp_path):
        paths = export_all(export_db, "s1", tmp_path)
        assert len(paths) == 3
        for name, path in paths.items():
            assert path.exists(), f"{name} not created"
