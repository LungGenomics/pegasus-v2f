"""Tests for integration scoring."""

import duckdb
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.scoring import compute_locus_gene_scores


@pytest.fixture
def scored_db():
    """DB with synthetic studies, loci, genes, and evidence for scoring."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)

    # Study
    c.execute("INSERT INTO studies (study_id, trait) VALUES ('s1', 'HEIGHT')")

    # Locus on chr1
    c.execute(
        "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position, "
        "lead_pvalue) VALUES ('l1', 's1', '1', 900000, 1100000, 1e-10)"
    )

    # Genes table (GENE_A near locus center, GENE_B farther)
    c.execute(
        "INSERT INTO genes (gene_symbol, chromosome, start_position, end_position) VALUES "
        "('GENE_A', '1', 950000, 1050000), "
        "('GENE_B', '1', 1080000, 1120000)"
    )

    # Evidence: GENE_A has GWAS + COLOC, GENE_B has only GWAS
    c.execute(
        "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
        "VALUES ('l1', 'GENE_A', 'GWAS', 'src1')"
    )
    c.execute(
        "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag, score) "
        "VALUES ('l1', 'GENE_A', 'COLOC', 'src2', 0.95)"
    )
    c.execute(
        "INSERT INTO locus_gene_evidence (locus_id, gene_symbol, evidence_category, source_tag) "
        "VALUES ('l1', 'GENE_B', 'GWAS', 'src1')"
    )

    yield c
    c.close()


SCORING_CONFIG = {
    "pegasus": {
        "study": {"id_prefix": "test", "traits": ["HEIGHT"]},
        "integration": {
            "method": "criteria_count_v1",
            "effector_threshold": 0.25,
            "criteria": [
                {"name": "coloc", "category": "COLOC", "threshold_field": "score", "threshold": 0.8},
            ],
        },
    },
}


class TestComputeScores:
    def test_basic_scoring(self, scored_db):
        n = compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        assert n == 2  # GENE_A + GENE_B

        rows = scored_db.execute(
            "SELECT gene_symbol, integration_rank, integration_score "
            "FROM locus_gene_scores ORDER BY integration_rank"
        ).fetchall()
        assert len(rows) == 2
        # GENE_A should rank higher (has COLOC criterion met)
        assert rows[0][0] == "GENE_A"
        assert rows[0][1] == 1
        assert rows[1][0] == "GENE_B"
        assert rows[1][1] == 2

    def test_nearest_gene(self, scored_db):
        compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        nearest = scored_db.execute(
            "SELECT gene_symbol FROM locus_gene_scores WHERE is_nearest_gene = TRUE"
        ).fetchall()
        assert len(nearest) == 1
        # GENE_A is closer to locus center
        assert nearest[0][0] == "GENE_A"

    def test_within_locus(self, scored_db):
        compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        within = scored_db.execute(
            "SELECT gene_symbol, is_within_locus FROM locus_gene_scores ORDER BY gene_symbol"
        ).fetchall()
        # GENE_A (950k-1050k) is within locus (900k-1100k)
        assert within[0][0] == "GENE_A"
        assert within[0][1] is True

    def test_predicted_effector(self, scored_db):
        compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        effectors = scored_db.execute(
            "SELECT gene_symbol FROM locus_gene_scores WHERE is_predicted_effector = TRUE"
        ).fetchall()
        assert len(effectors) >= 1

    def test_no_loci_returns_zero(self):
        c = duckdb.connect(":memory:")
        create_pegasus_schema(c)
        n = compute_locus_gene_scores(c, SCORING_CONFIG)
        assert n == 0
        c.close()

    def test_idempotent(self, scored_db):
        compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        compute_locus_gene_scores(scored_db, SCORING_CONFIG)
        rows = scored_db.execute("SELECT COUNT(*) FROM locus_gene_scores").fetchone()
        assert rows[0] == 2  # Same count, not doubled
