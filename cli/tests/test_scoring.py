"""Tests for materialization and scoring (scored_evidence)."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.scoring import materialize_scored_evidence


@pytest.fixture
def scoring_db():
    """DB with PEGASUS schema, genes, studies, loci, and evidence."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    # Genes with coordinates
    c.execute(
        "INSERT INTO genes (gene_symbol, ensembl_gene_id, chromosome, start_position, end_position) "
        "VALUES ('GENE_A', 'ENSG00001', '1', 950000, 1050000), "
        "       ('GENE_B', 'ENSG00002', '1', 1080000, 1120000), "
        "       ('GENE_C', 'ENSG00003', '2', 500000, 600000)"
    )

    # Study + locus
    c.execute(
        "INSERT INTO studies (study_id, study_name, trait) VALUES ('s1_height', 's1', 'HEIGHT')"
    )
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, lead_pvalue) "
        "VALUES ('l1', 's1_height', '1', 900000, 1100000, 1e-10)"
    )

    # Gene-level evidence (no chr/pos)
    c.execute(
        "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
        "VALUES ('GENE_A', 'GWAS', 'src1'), "
        "       ('GENE_A', 'KNOW', 'hpa'), "
        "       ('GENE_B', 'GWAS', 'src1')"
    )

    # Variant-level evidence (with chr/pos)
    c.execute(
        "INSERT INTO evidence (gene_symbol, chromosome, position, evidence_category, source_tag) "
        "VALUES ('GENE_A', '1', 1000000, 'QTL', 'eqtl1')"
    )

    yield c
    c.close()


class TestMaterializeScored:
    def test_basic_scoring(self, scoring_db):
        config = {"pegasus": {"study": [{"id_prefix": "s1", "traits": ["HEIGHT"]}]}}
        n = materialize_scored_evidence(scoring_db, config)
        assert n > 0

        rows = scoring_db.execute("SELECT * FROM scored_evidence").fetchall()
        assert len(rows) == n

    def test_gene_level_match_type(self, scoring_db):
        """Gene-level evidence matched as match_type='gene'."""
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        rows = scoring_db.execute(
            "SELECT match_type FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_A' AND evidence_category = 'KNOW'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "gene"

    def test_variant_level_match_type(self, scoring_db):
        """Variant-level evidence matched as match_type='position'."""
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        rows = scoring_db.execute(
            "SELECT match_type FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_A' AND evidence_category = 'QTL'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0][0] == "position"

    def test_candidate_genes_from_geometry(self, scoring_db):
        """Genes overlapping the locus window appear as candidates."""
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        genes = scoring_db.execute(
            "SELECT DISTINCT gene_symbol FROM scored_evidence WHERE locus_id = 'l1'"
        ).fetchall()
        gene_set = {r[0] for r in genes}
        # GENE_A (950k-1050k) overlaps locus (900k-1100k)
        assert "GENE_A" in gene_set

    def test_n_candidate_genes_populated(self, scoring_db):
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        rows = scoring_db.execute(
            "SELECT DISTINCT n_candidate_genes FROM scored_evidence WHERE locus_id = 'l1'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0][0] > 0

    def test_integration_rank(self, scoring_db):
        """Genes are ranked — GENE_A should rank higher (more evidence)."""
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        # GENE_A has GWAS + KNOW + QTL = 3 categories
        # GENE_B has GWAS = 1 category
        rank_a = scoring_db.execute(
            "SELECT DISTINCT integration_rank FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_A' AND locus_id = 'l1'"
        ).fetchone()
        rank_b = scoring_db.execute(
            "SELECT DISTINCT integration_rank FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_B' AND locus_id = 'l1'"
        ).fetchone()

        assert rank_a is not None
        assert rank_b is not None
        assert rank_a[0] < rank_b[0]  # lower rank = better

    def test_predicted_effector(self, scoring_db):
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        effector = scoring_db.execute(
            "SELECT DISTINCT is_predicted_effector FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_A' AND locus_id = 'l1'"
        ).fetchone()
        assert effector[0] is True

    def test_idempotent(self, scoring_db):
        config = {"pegasus": {}}
        n1 = materialize_scored_evidence(scoring_db, config)
        n2 = materialize_scored_evidence(scoring_db, config)
        assert n1 == n2

    def test_no_loci_returns_zero(self, scoring_db):
        """When no loci exist, returns 0."""
        scoring_db.execute("DELETE FROM loci")
        config = {"pegasus": {}}
        n = materialize_scored_evidence(scoring_db, config)
        assert n == 0

    def test_incremental_rescore(self, scoring_db):
        """Incremental rescore only affects target study."""
        # Add a second study
        scoring_db.execute(
            "INSERT INTO studies (study_id, study_name, trait) VALUES ('s2_fvc', 's2', 'FVC')"
        )
        scoring_db.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l2', 's2_fvc', '2', 400000, 700000)"
        )
        scoring_db.execute(
            "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
            "VALUES ('GENE_C', 'EXP', 'expr1')"
        )

        config = {"pegasus": {}}

        # Score all first
        materialize_scored_evidence(scoring_db, config)
        total = scoring_db.execute("SELECT COUNT(*) FROM scored_evidence").fetchone()[0]
        assert total > 0

        # Now rescore only s1
        materialize_scored_evidence(scoring_db, config, study_name="s1")

        # s2 evidence should still be there
        s2_count = scoring_db.execute(
            "SELECT COUNT(*) FROM scored_evidence WHERE study_id = 's2_fvc'"
        ).fetchone()[0]
        assert s2_count > 0

    def test_distal_gene_from_variant_evidence(self, scoring_db):
        """Variant evidence naming a gene outside the locus window still appears."""
        # Add variant evidence with a gene not in the locus window geometry
        scoring_db.execute(
            "INSERT INTO evidence (gene_symbol, chromosome, position, evidence_category, source_tag) "
            "VALUES ('DISTAL_GENE', '1', 950000, 'QTL', 'distal_eqtl')"
        )

        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        genes = scoring_db.execute(
            "SELECT DISTINCT gene_symbol FROM scored_evidence WHERE locus_id = 'l1'"
        ).fetchall()
        gene_set = {r[0] for r in genes}
        assert "DISTAL_GENE" in gene_set

    def test_candidate_gene_no_evidence(self, scoring_db):
        """Candidate gene with no evidence still appears in scored_evidence."""
        # GENE_B overlaps the locus — remove all its evidence
        scoring_db.execute("DELETE FROM evidence WHERE gene_symbol = 'GENE_B'")

        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        # GENE_B should still appear (it overlaps the locus geometrically)
        rows = scoring_db.execute(
            "SELECT gene_symbol, evidence_category FROM scored_evidence "
            "WHERE gene_symbol = 'GENE_B' AND locus_id = 'l1'"
        ).fetchall()
        assert len(rows) >= 1

    def test_loci_n_candidate_genes_updated(self, scoring_db):
        config = {"pegasus": {}}
        materialize_scored_evidence(scoring_db, config)

        n = scoring_db.execute(
            "SELECT n_candidate_genes FROM loci WHERE locus_id = 'l1'"
        ).fetchone()[0]
        assert n > 0
