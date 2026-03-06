"""Tests for the unified evidence loader."""

import duckdb
import pandas as pd
import pytest

from pegasus_v2f.evidence_loader import load_evidence, load_all_evidence
from pegasus_v2f.pegasus_schema import create_pegasus_schema


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)
    yield c
    c.close()


def _make_source(name="test_src", evidence_blocks=None):
    return {
        "name": name,
        "source_type": "file",
        "evidence": evidence_blocks or [],
    }


class TestLoadEvidenceGeneLeve:
    def test_gene_level_inserts(self, conn):
        """Gene-level evidence (no chr/pos) inserts correctly."""
        df = pd.DataFrame({
            "gene": ["TP53", "BRCA1", "EGFR"],
            "n_evidence": [5.0, 3.0, 7.0],
        })
        source = _make_source()
        block = {
            "category": "KNOW",
            "source_tag": "test_know",
            "fields": {"gene": "gene", "score": "n_evidence"},
        }
        result = load_evidence(conn, source, df, block)

        assert result["rows_inserted"] == 3
        assert result["category"] == "KNOW"
        assert result["source_tag"] == "test_know"

        rows = conn.execute("SELECT gene_symbol, score, chromosome, position FROM evidence").fetchall()
        assert len(rows) == 3
        # Gene-level: chromosome and position should be null
        for row in rows:
            assert row[2] is None  # chromosome
            assert row[3] is None  # position

    def test_gene_level_with_pvalue(self, conn):
        df = pd.DataFrame({
            "gene": ["AGER", "MUC5B"],
            "min_P": [1e-10, 5e-8],
        })
        block = {
            "category": "GWAS",
            "source_tag": "test_gwas_genes",
            "fields": {"gene": "gene", "pvalue": "min_P"},
        }
        result = load_evidence(conn, _make_source(), df, block)
        assert result["rows_inserted"] == 2

        rows = conn.execute("SELECT gene_symbol, pvalue FROM evidence ORDER BY pvalue").fetchall()
        assert rows[0][0] == "AGER"
        assert rows[0][1] == pytest.approx(1e-10)


class TestLoadEvidenceVariantLevel:
    def test_variant_level_inserts(self, conn):
        """Variant-level evidence (with chr/pos) inserts correctly."""
        df = pd.DataFrame({
            "gene": ["AGER", "AGER"],
            "chr": ["6", "6"],
            "pos": [32180000, 32185000],
            "pval": [1e-12, 5e-8],
            "rsid": ["rs123", "rs456"],
        })
        block = {
            "category": "QTL",
            "source_tag": "eqtl_lung",
            "fields": {
                "gene": "gene",
                "chromosome": "chr",
                "position": "pos",
                "pvalue": "pval",
                "rsid": "rsid",
            },
        }
        result = load_evidence(conn, _make_source(), df, block)
        assert result["rows_inserted"] == 2

        rows = conn.execute(
            "SELECT gene_symbol, chromosome, position, rsid FROM evidence ORDER BY position"
        ).fetchall()
        assert rows[0] == ("AGER", "6", 32180000, "rs123")
        assert rows[1] == ("AGER", "6", 32185000, "rs456")


class TestTraitTagging:
    def test_source_level_traits_list(self, conn):
        """Source-level traits list propagated to rows."""
        df = pd.DataFrame({"gene": ["TP53", "BRCA1"]})
        block = {
            "category": "KNOW",
            "source_tag": "test_traits",
            "traits": ["COPD", "asthma"],
            "fields": {"gene": "gene"},
        }
        load_evidence(conn, _make_source(), df, block)

        rows = conn.execute("SELECT trait FROM evidence").fetchall()
        assert all(row[0] == "COPD, asthma" for row in rows)

    def test_per_row_trait_column(self, conn):
        """Per-row trait field mapping works."""
        df = pd.DataFrame({
            "gene": ["TP53", "BRCA1", "EGFR"],
            "trait": ["FEV1", "FVC", "FEV1"],
        })
        block = {
            "category": "GWAS",
            "source_tag": "test_trait_col",
            "fields": {"gene": "gene", "trait": "trait"},
        }
        load_evidence(conn, _make_source(), df, block)

        rows = conn.execute(
            "SELECT gene_symbol, trait FROM evidence ORDER BY gene_symbol"
        ).fetchall()
        assert rows[0] == ("BRCA1", "FVC")
        assert rows[1] == ("EGFR", "FEV1")
        assert rows[2] == ("TP53", "FEV1")

    def test_no_trait_is_null(self, conn):
        """Evidence without trait tags has null trait."""
        df = pd.DataFrame({"gene": ["TP53"]})
        block = {
            "category": "KNOW",
            "source_tag": "test_no_trait",
            "fields": {"gene": "gene"},
        }
        load_evidence(conn, _make_source(), df, block)

        row = conn.execute("SELECT trait FROM evidence").fetchone()
        assert row[0] is None


class TestReload:
    def test_reload_cleans_and_reinserts(self, conn):
        """Re-loading cleans up old evidence and re-inserts."""
        df = pd.DataFrame({"gene": ["TP53", "BRCA1"]})
        block = {
            "category": "KNOW",
            "source_tag": "test_reload",
            "fields": {"gene": "gene"},
        }
        source = _make_source()

        load_evidence(conn, source, df, block)
        assert conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 2

        # Reload with different data
        df2 = pd.DataFrame({"gene": ["EGFR"]})
        load_evidence(conn, source, df2, block)
        assert conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1

        row = conn.execute("SELECT gene_symbol FROM evidence").fetchone()
        assert row[0] == "EGFR"


class TestMultiEvidence:
    def test_multi_evidence_blocks(self, conn):
        """Multiple evidence blocks from single source."""
        df = pd.DataFrame({
            "gene": ["AGER", "MUC5B"],
            "n_evidence": [5.0, 3.0],
            "min_P": [1e-10, 5e-8],
        })
        source = _make_source(evidence_blocks=[
            {
                "category": "GWAS",
                "source_tag": "test_n_evidence",
                "fields": {"gene": "gene", "score": "n_evidence"},
            },
            {
                "category": "GWAS",
                "source_tag": "test_min_p",
                "fields": {"gene": "gene", "pvalue": "min_P"},
            },
        ])

        results = load_all_evidence(conn, source, df)
        assert len(results) == 2
        assert results[0]["rows_inserted"] == 2
        assert results[1]["rows_inserted"] == 2

        total = conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]
        assert total == 4

        # Each source_tag is distinct
        tags = conn.execute(
            "SELECT DISTINCT source_tag FROM evidence ORDER BY source_tag"
        ).fetchall()
        assert [t[0] for t in tags] == ["test_min_p", "test_n_evidence"]


class TestDataSourcesProvenance:
    def test_provenance_updated(self, conn):
        df = pd.DataFrame({"gene": ["TP53"]})
        block = {
            "category": "KNOW",
            "source_tag": "prov_test",
            "fields": {"gene": "gene"},
        }
        load_evidence(conn, _make_source(), df, block)

        row = conn.execute(
            "SELECT source_tag, source_name, evidence_category, record_count "
            "FROM data_sources WHERE source_tag = 'prov_test'"
        ).fetchone()
        assert row[0] == "prov_test"
        assert row[1] == "test_src"
        assert row[2] == "KNOW"
        assert row[3] == 1


class TestSkipsInvalidGenes:
    def test_skips_nan_genes(self, conn):
        df = pd.DataFrame({"gene": ["TP53", None, "nan"]})
        block = {
            "category": "KNOW",
            "source_tag": "skip_test",
            "fields": {"gene": "gene"},
        }
        load_evidence(conn, _make_source(), df, block)
        assert conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
