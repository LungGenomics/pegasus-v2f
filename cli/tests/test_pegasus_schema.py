"""Tests for PEGASUS schema DDL and evidence categories."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import (
    EVIDENCE_CATEGORIES,
    PEGASUS_DDL,
    create_pegasus_schema,
)


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


class TestEvidenceCategories:
    def test_has_core_categories(self):
        for cat in ["QTL", "COLOC", "GWAS", "PROX", "EXP", "KNOW"]:
            assert cat in EVIDENCE_CATEGORIES

    def test_all_values_are_strings(self):
        for k, v in EVIDENCE_CATEGORIES.items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_category_count(self):
        assert len(EVIDENCE_CATEGORIES) == 22


class TestCreatePegasusSchema:
    def test_creates_all_tables(self, conn):
        create_pegasus_schema(conn)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        expected = {
            "genes",
            "variants",
            "studies",
            "loci",
            "locus_gene_evidence",
            "gene_evidence",
            "locus_gene_scores",
            "data_sources",
        }
        assert expected == tables

    def test_idempotent(self, conn):
        create_pegasus_schema(conn)
        create_pegasus_schema(conn)  # should not raise
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert len(tables) == 8

    def test_genes_table_columns(self, conn):
        create_pegasus_schema(conn)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'genes'"
            ).fetchall()
        }
        assert "gene_symbol" in cols
        assert "ensembl_gene_id" in cols
        assert "chromosome" in cols

    def test_loci_table_has_locus_source(self, conn):
        create_pegasus_schema(conn)
        cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'loci'"
            ).fetchall()
        }
        assert "locus_source" in cols

    def test_locus_gene_evidence_identity(self, conn):
        """Auto-increment ID works for locus_gene_evidence."""
        create_pegasus_schema(conn)
        # Need a study and locus first (foreign keys)
        conn.execute(
            "INSERT INTO studies (study_id, trait) VALUES ('test_study', 'HEIGHT')"
        )
        conn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('locus_1', 'test_study', '1', 1000000, 2000000)"
        )
        conn.execute(
            "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
            "VALUES ('locus_1', 'GENE_A', 'GWAS', 'test_src')"
        )
        conn.execute(
            "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
            "VALUES ('locus_1', 'GENE_B', 'GWAS', 'test_src')"
        )
        rows = conn.execute(
            "SELECT id FROM locus_gene_evidence ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] != rows[1][0]  # different IDs

    def test_unique_constraint_locus_gene_evidence(self, conn):
        create_pegasus_schema(conn)
        conn.execute(
            "INSERT INTO studies (study_id, trait) VALUES ('s1', 'FEV1')"
        )
        conn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 100, 200)"
        )
        conn.execute(
            "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
            "VALUES ('l1', 'G1', 'QTL', 'src1')"
        )
        with pytest.raises(duckdb.ConstraintException):
            conn.execute(
                "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
                "VALUES ('l1', 'G1', 'QTL', 'src1')"
            )

    def test_data_sources_table(self, conn):
        create_pegasus_schema(conn)
        conn.execute(
            "INSERT INTO data_sources (source_tag, source_name, is_integrated) "
            "VALUES ('tag1', 'Test Source', TRUE)"
        )
        row = conn.execute(
            "SELECT source_tag, source_name, is_integrated FROM data_sources"
        ).fetchone()
        assert row[0] == "tag1"
        assert row[1] == "Test Source"
        assert row[2] is True

    def test_loci_default_locus_source(self, conn):
        create_pegasus_schema(conn)
        conn.execute(
            "INSERT INTO studies (study_id, trait) VALUES ('s1', 'FEV1')"
        )
        conn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 100, 200)"
        )
        row = conn.execute("SELECT locus_source FROM loci").fetchone()
        assert row[0] == "curated"


class TestCreateSchemaWithPegasus:
    def test_pegasus_tables_created_with_config(self, conn):
        from pegasus_v2f.db_schema import create_schema

        config = {"pegasus": {"study": {"id_prefix": "test"}}}
        create_schema(conn, config=config)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "genes" in tables
        assert "loci" in tables
        assert "locus_gene_evidence" in tables

    def test_no_pegasus_tables_without_config(self, conn):
        from pegasus_v2f.db_schema import create_schema

        create_schema(conn)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "genes" not in tables
        assert "loci" not in tables
