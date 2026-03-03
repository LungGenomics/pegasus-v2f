"""Tests for PEGASUS search index and gene evidence summary."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.annotate import (
    create_pegasus_search_index,
    create_gene_evidence_summary,
)


@pytest.fixture
def pegasus_db():
    """DB with PEGASUS schema and synthetic evidence data."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    # Studies + loci
    c.execute("INSERT INTO studies (study_id, trait) VALUES ('s1', 'HEIGHT')")
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1', '1', 900000, 1100000, 1e-10)"
    )

    # Genes
    c.execute(
        "INSERT INTO genes (gene_symbol, ensembl_gene_id, chromosome, start_position, end_position) "
        "VALUES ('GENE_A', 'ENSG00001', '1', 950000, 1050000), "
        "       ('GENE_B', 'ENSG00002', '1', 1080000, 1120000)"
    )

    # Locus-gene evidence
    c.execute(
        "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
        "VALUES ('l1', 'GENE_A', 'GWAS', 'src1'), "
        "       ('l1', 'GENE_A', 'COLOC', 'src2'), "
        "       ('l1', 'GENE_B', 'GWAS', 'src1')"
    )

    # Gene-level evidence
    c.execute(
        "INSERT INTO gene_evidence (gene_symbol, evidence_category, evidence_type, source_tag) "
        "VALUES ('GENE_A', 'KNOW', 'secretome', 'hpa')"
    )

    # Scores
    c.execute(
        "INSERT INTO locus_gene_scores VALUES "
        "('l1', 'GENE_A', 10.0, TRUE, TRUE, 'criteria_count_v1', 2.1, 1, TRUE), "
        "('l1', 'GENE_B', 50.0, FALSE, TRUE, 'criteria_count_v1', 1.1, 2, FALSE)"
    )

    yield c
    c.close()


class TestPegasusSearchIndex:
    def test_creates_table(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        tables = [r[0] for r in pegasus_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()]
        assert "gene_search_index" in tables

    def test_has_all_genes(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        rows = pegasus_db.execute(
            "SELECT gene_symbol FROM gene_search_index ORDER BY gene_symbol"
        ).fetchall()
        genes = [r[0] for r in rows]
        assert "GENE_A" in genes
        assert "GENE_B" in genes

    def test_includes_ensembl_id(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        row = pegasus_db.execute(
            "SELECT ensembl_gene_id FROM gene_search_index WHERE gene_symbol = 'GENE_A'"
        ).fetchone()
        assert row[0] == "ENSG00001"

    def test_includes_evidence_categories(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        row = pegasus_db.execute(
            "SELECT evidence_categories FROM gene_search_index WHERE gene_symbol = 'GENE_A'"
        ).fetchone()
        cats = row[0]
        assert "GWAS" in cats
        assert "COLOC" in cats
        assert "KNOW" in cats

    def test_includes_score_info(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        row = pegasus_db.execute(
            "SELECT best_rank, is_any_effector, n_loci FROM gene_search_index "
            "WHERE gene_symbol = 'GENE_A'"
        ).fetchone()
        assert row[0] == 1  # best_rank
        assert row[1] == 1  # is_any_effector
        assert row[2] == 1  # n_loci

    def test_searchable_text(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        row = pegasus_db.execute(
            "SELECT searchable_text FROM gene_search_index WHERE gene_symbol = 'GENE_A'"
        ).fetchone()
        text = row[0]
        assert "GENE_A" in text
        assert "ENSG00001" in text

    def test_idempotent(self, pegasus_db):
        create_pegasus_search_index(pegasus_db)
        create_pegasus_search_index(pegasus_db)
        count = pegasus_db.execute("SELECT COUNT(*) FROM gene_search_index").fetchone()[0]
        assert count == 2


class TestGeneEvidenceSummary:
    def test_creates_table(self, pegasus_db):
        create_gene_evidence_summary(pegasus_db)
        tables = [r[0] for r in pegasus_db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()]
        assert "gene_evidence_summary" in tables

    def test_has_locus_level_evidence(self, pegasus_db):
        create_gene_evidence_summary(pegasus_db)
        rows = pegasus_db.execute(
            "SELECT * FROM gene_evidence_summary "
            "WHERE gene_symbol = 'GENE_A' AND evidence_level = 'locus'"
        ).fetchall()
        assert len(rows) == 2  # GWAS + COLOC

    def test_has_gene_level_evidence(self, pegasus_db):
        create_gene_evidence_summary(pegasus_db)
        rows = pegasus_db.execute(
            "SELECT * FROM gene_evidence_summary "
            "WHERE gene_symbol = 'GENE_A' AND evidence_level = 'gene'"
        ).fetchall()
        assert len(rows) == 1  # KNOW

    def test_record_counts(self, pegasus_db):
        create_gene_evidence_summary(pegasus_db)
        row = pegasus_db.execute(
            "SELECT record_count FROM gene_evidence_summary "
            "WHERE gene_symbol = 'GENE_A' AND evidence_category = 'GWAS' AND evidence_level = 'locus'"
        ).fetchone()
        assert row[0] == 1

    def test_idempotent(self, pegasus_db):
        create_gene_evidence_summary(pegasus_db)
        create_gene_evidence_summary(pegasus_db)
        count = pegasus_db.execute("SELECT COUNT(*) FROM gene_evidence_summary").fetchone()[0]
        assert count == 4  # 3 locus-level + 1 gene-level
