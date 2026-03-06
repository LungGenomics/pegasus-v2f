"""Tests for PEGASUS search index and gene annotations."""

import duckdb
import pytest
from unittest.mock import patch

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.annotate import (
    create_pegasus_search_index,
    create_gene_annotations,
)


@pytest.fixture
def pegasus_db():
    """DB with PEGASUS schema and synthetic evidence data."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    # Studies + loci
    c.execute(
        "INSERT INTO studies (study_id, study_name, trait) VALUES ('s1', 'test', 'HEIGHT')"
    )
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

    # Evidence (unified table)
    c.execute(
        "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
        "VALUES ('GENE_A', 'GWAS', 'src1'), "
        "       ('GENE_A', 'COLOC', 'src2'), "
        "       ('GENE_B', 'GWAS', 'src1'), "
        "       ('GENE_A', 'KNOW', 'hpa')"
    )

    # Scored evidence
    c.execute(
        "INSERT INTO scored_evidence (locus_id, study_id, gene_symbol, evidence_category, "
        "source_tag, match_type, integration_rank, is_predicted_effector, n_candidate_genes) "
        "VALUES ('l1', 's1', 'GENE_A', 'GWAS', 'src1', 'gene', 1, TRUE, 2), "
        "       ('l1', 's1', 'GENE_B', 'GWAS', 'src1', 'gene', 2, FALSE, 2)"
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


class TestGeneAnnotationsPegasus:
    """Test that create_gene_annotations populates the PEGASUS genes table."""

    def _mock_ensembl(self, genes, ga_config):
        """Return a DataFrame mimicking Ensembl REST response."""
        import pandas as pd

        data = {
            "GENE_A": {
                "gene": "GENE_A",
                "ensembl_gene_id": "ENSG00001",
                "gene_name": "Gene Alpha",
                "chromosome": "1",
                "start_position": 950000,
                "end_position": 1050000,
                "strand": 1,
            },
            "GENE_B": {
                "gene": "GENE_B",
                "ensembl_gene_id": "ENSG00002",
                "gene_name": "Gene Beta",
                "chromosome": "1",
                "start_position": 1080000,
                "end_position": 1120000,
                "strand": -1,
            },
        }
        rows = [data[g] for g in genes if g in data]
        return pd.DataFrame(rows)

    @patch("pegasus_v2f.annotate._fetch_ensembl_genes")
    def test_populates_genes_table(self, mock_fetch, pegasus_db):
        mock_fetch.side_effect = self._mock_ensembl
        pegasus_db.execute("DELETE FROM genes")

        create_gene_annotations(pegasus_db, ["GENE_A", "GENE_B"], {})

        rows = pegasus_db.execute(
            "SELECT gene_symbol, ensembl_gene_id, gene_name FROM genes ORDER BY gene_symbol"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "GENE_A"
        assert rows[0][1] == "ENSG00001"
        assert rows[0][2] == "Gene Alpha"

    @patch("pegasus_v2f.annotate._fetch_ensembl_genes")
    def test_upsert_does_not_duplicate(self, mock_fetch, pegasus_db):
        mock_fetch.side_effect = self._mock_ensembl
        pegasus_db.execute("DELETE FROM genes")

        create_gene_annotations(pegasus_db, ["GENE_A"], {})
        create_gene_annotations(pegasus_db, ["GENE_A"], {})

        count = pegasus_db.execute("SELECT COUNT(*) FROM genes").fetchone()[0]
        assert count == 1

    @patch("pegasus_v2f.annotate._fetch_ensembl_genes")
    def test_captures_gene_name(self, mock_fetch, pegasus_db):
        mock_fetch.side_effect = self._mock_ensembl
        pegasus_db.execute("DELETE FROM genes")

        create_gene_annotations(pegasus_db, ["GENE_B"], {})

        row = pegasus_db.execute(
            "SELECT gene_name FROM genes WHERE gene_symbol = 'GENE_B'"
        ).fetchone()
        assert row[0] == "Gene Beta"
