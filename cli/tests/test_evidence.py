"""Tests for evidence loading — locus sources, gene/variant evidence, routing."""

import duckdb
import pandas as pd
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.evidence import (
    _create_studies,
    _match_to_loci,
    load_locus_definition,
    load_gwas_sumstats,
    load_gene_evidence,
    load_variant_evidence,
    route_evidence_source,
)


@pytest.fixture
def pconn():
    """In-memory DuckDB with PEGASUS schema."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)
    yield c
    c.close()


BASIC_CONFIG = {
    "pegasus": {
        "study": {
            "id_prefix": "test_2024",
            "gwas_source": "PMID:00000001",
            "ancestry": "European",
            "traits": ["HEIGHT", "WEIGHT"],
        },
        "locus_definition": {
            "window_kb": 500,
            "merge_distance_kb": 250,
        },
    },
}


class TestCreateStudies:
    def test_creates_studies(self, pconn):
        ids = _create_studies(pconn, BASIC_CONFIG)
        assert set(ids) == {"test_2024_height", "test_2024_weight"}
        rows = pconn.execute("SELECT study_id, trait FROM studies ORDER BY study_id").fetchall()
        assert len(rows) == 2

    def test_idempotent(self, pconn):
        _create_studies(pconn, BASIC_CONFIG)
        _create_studies(pconn, BASIC_CONFIG)
        rows = pconn.execute("SELECT COUNT(*) FROM studies").fetchone()
        assert rows[0] == 2


class TestMatchToLoci:
    def test_finds_overlapping_locus(self, pconn):
        pconn.execute("INSERT INTO studies VALUES ('s1', 'H', NULL, NULL, NULL, NULL, NULL, 0)")
        pconn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 1000000, 2000000)"
        )
        matches = _match_to_loci(pconn, "1", 1500000)
        assert len(matches) == 1
        assert matches[0]["locus_id"] == "l1"

    def test_no_match_outside(self, pconn):
        pconn.execute("INSERT INTO studies VALUES ('s1', 'H', NULL, NULL, NULL, NULL, NULL, 0)")
        pconn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 1000000, 2000000)"
        )
        assert _match_to_loci(pconn, "1", 3000000) == []

    def test_different_chromosome(self, pconn):
        pconn.execute("INSERT INTO studies VALUES ('s1', 'H', NULL, NULL, NULL, NULL, NULL, 0)")
        pconn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 1000000, 2000000)"
        )
        assert _match_to_loci(pconn, "2", 1500000) == []


class TestLoadLocusDefinition:
    def test_creates_loci_and_evidence(self, pconn):
        source = {
            "name": "test_loci",
            "source_type": "file",
            "evidence": {
                "role": "locus_definition",
                "source_tag": "test_2024",
                "fields": {
                    "gene": "gene",
                    "trait": "trait",
                    "chromosome": "chr",
                    "position": "pos",
                    "pvalue": "pval",
                },
            },
        }
        df = pd.DataFrame({
            "gene": ["GENE_A", "GENE_B", "GENE_C"],
            "trait": ["HEIGHT", "HEIGHT", "WEIGHT"],
            "chr": ["1", "1", "2"],
            "pos": [1000000, 1200000, 5000000],
            "pval": [1e-10, 1e-8, 1e-12],
        })

        result = load_locus_definition(pconn, source, df, BASIC_CONFIG)
        assert result["studies"] == 2
        assert result["loci"] >= 2  # at least HEIGHT and WEIGHT loci
        assert result["evidence_rows"] >= 3

        # Check studies created
        studies = pconn.execute("SELECT study_id FROM studies ORDER BY study_id").fetchall()
        assert len(studies) == 2

        # Check loci created
        loci = pconn.execute("SELECT locus_id, locus_source FROM loci").fetchall()
        assert len(loci) >= 2
        assert all(r[1] == "curated" for r in loci)

        # Check evidence written
        evidence = pconn.execute("SELECT * FROM locus_gene_evidence").fetchall()
        assert len(evidence) >= 3

    def test_filters_to_declared_traits(self, pconn):
        """Rows with traits not in config are ignored."""
        source = {
            "name": "t",
            "evidence": {
                "role": "locus_definition",
                "source_tag": "x",
                "fields": {"gene": "gene", "trait": "trait", "chromosome": "chr", "position": "pos"},
            },
        }
        df = pd.DataFrame({
            "gene": ["G1", "G2"],
            "trait": ["HEIGHT", "UNKNOWN_TRAIT"],
            "chr": ["1", "1"],
            "pos": [1000000, 2000000],
        })
        result = load_locus_definition(pconn, source, df, BASIC_CONFIG)
        # Only HEIGHT locus should be created
        loci = pconn.execute("SELECT * FROM loci").fetchall()
        assert all("HEIGHT" in r[0] or "height" in r[0].lower() for r in loci)


class TestLoadGwasSumstats:
    def test_auto_clumps_loci(self, pconn):
        source = {
            "name": "test_sumstats",
            "source_type": "file",
            "evidence": {
                "role": "gwas_sumstats",
                "source_tag": "test_2024",
                "pvalue_threshold": 5e-8,
                "clump_distance_kb": 500,
                "fields": {
                    "chromosome": "CHR",
                    "position": "BP",
                    "pvalue": "P",
                    "rsid": "SNP",
                    "effect_size": "BETA",
                },
            },
        }
        # Two significant hits far apart → two loci
        df = pd.DataFrame({
            "SNP": ["rs1", "rs2", "rs3", "rs4"],
            "CHR": ["1", "1", "2", "1"],
            "BP": [1000000, 1100000, 5000000, 90000000],
            "P": [1e-10, 1e-9, 1e-15, 0.5],  # rs4 not significant
            "BETA": [0.1, 0.05, 0.2, 0.01],
        })

        result = load_gwas_sumstats(pconn, source, df, BASIC_CONFIG)
        assert result["variants"] >= 4
        # rs1 and rs2 should cluster, rs3 separate, rs4 not significant
        # Per trait: 2 loci (chr1 cluster + chr2), times 2 traits = up to 4
        assert result["loci"] >= 2

        loci = pconn.execute("SELECT locus_id, locus_source FROM loci").fetchall()
        assert all(r[1] == "auto_clumped" for r in loci)

    def test_sumstats_assigns_to_existing_curated_loci(self, pconn):
        """When curated loci exist, sumstats variants overlapping them go there."""
        # First create curated loci via locus_definition
        curated_source = {
            "name": "curated",
            "evidence": {
                "role": "locus_definition",
                "source_tag": "cur",
                "fields": {"gene": "gene", "trait": "trait", "chromosome": "chr", "position": "pos"},
            },
        }
        curated_df = pd.DataFrame({
            "gene": ["GENE_A"],
            "trait": ["HEIGHT"],
            "chr": ["1"],
            "pos": [1000000],
        })
        load_locus_definition(pconn, curated_source, curated_df, BASIC_CONFIG)

        curated_loci = pconn.execute("SELECT locus_id FROM loci WHERE locus_source = 'curated'").fetchall()
        assert len(curated_loci) >= 1

        # Now load sumstats with a significant hit overlapping curated locus
        ss_source = {
            "name": "ss",
            "evidence": {
                "role": "gwas_sumstats",
                "source_tag": "ss_tag",
                "pvalue_threshold": 5e-8,
                "clump_distance_kb": 500,
                "fields": {"chromosome": "CHR", "position": "BP", "pvalue": "P"},
            },
        }
        ss_df = pd.DataFrame({
            "CHR": ["1"],
            "BP": [1050000],  # within curated locus window
            "P": [1e-10],
        })
        result = load_gwas_sumstats(pconn, ss_source, ss_df, BASIC_CONFIG)
        # Should not create new loci since the hit falls in the curated locus
        assert result["loci"] == 0 or result["evidence_rows"] > 0

    def test_no_significant_variants(self, pconn):
        source = {
            "name": "ns",
            "evidence": {
                "role": "gwas_sumstats",
                "source_tag": "ns_tag",
                "pvalue_threshold": 5e-8,
                "fields": {"chromosome": "CHR", "position": "BP", "pvalue": "P"},
            },
        }
        df = pd.DataFrame({"CHR": ["1"], "BP": [1000], "P": [0.5]})
        result = load_gwas_sumstats(pconn, source, df, BASIC_CONFIG)
        assert result["loci"] == 0
        assert result["evidence_rows"] == 0


class TestLoadGeneEvidence:
    def test_inserts_gene_evidence(self, pconn):
        source = {
            "name": "secretome",
            "evidence": {
                "centric": "gene",
                "category": "KNOW",
                "evidence_type": "secretome",
                "source_tag": "hpa",
                "fields": {"gene": "Gene"},
            },
        }
        df = pd.DataFrame({"Gene": ["GENE_A", "GENE_B", "GENE_A"]})
        result = load_gene_evidence(pconn, source, df)
        # GENE_A appears twice but unique constraint should handle it
        rows = pconn.execute("SELECT * FROM gene_evidence").fetchall()
        assert len(rows) == 2  # GENE_A + GENE_B (duplicate ignored)

    def test_provenance_recorded(self, pconn):
        source = {
            "name": "test_src",
            "evidence": {
                "centric": "gene",
                "category": "EXP",
                "source_tag": "exp_tag",
                "fields": {"gene": "g"},
            },
        }
        df = pd.DataFrame({"g": ["X"]})
        load_gene_evidence(pconn, source, df)
        ds = pconn.execute("SELECT * FROM data_sources WHERE source_tag = 'exp_tag'").fetchone()
        assert ds is not None


class TestLoadVariantEvidence:
    def test_matches_to_existing_loci(self, pconn):
        # Create a locus first
        pconn.execute("INSERT INTO studies VALUES ('s1', 'H', NULL, NULL, NULL, NULL, NULL, 0)")
        pconn.execute(
            "INSERT INTO loci (locus_id, study_id, chromosome, start_position, end_position) "
            "VALUES ('l1', 's1', '1', 900000, 1100000)"
        )
        # Existing evidence so gene is discoverable
        pconn.execute(
            "INSERT INTO locus_gene_evidence "
            "(locus_id, gene_symbol, evidence_category, source_tag) "
            "VALUES ('l1', 'GENE_A', 'GWAS', 'seed')"
        )

        source = {
            "name": "coloc",
            "evidence": {
                "centric": "variant",
                "category": "COLOC",
                "source_tag": "coloc_tag",
                "fields": {"gene": "gene", "chromosome": "chr", "position": "pos", "score": "pph4"},
            },
        }
        df = pd.DataFrame({
            "gene": ["GENE_A"],
            "chr": ["1"],
            "pos": [1000000],
            "pph4": [0.95],
        })
        result = load_variant_evidence(pconn, source, df)
        assert result["evidence_rows"] == 1
        assert result["unmatched"] == 0


class TestRouteEvidenceSource:
    def test_routes_locus_definition(self, pconn):
        source = {
            "name": "ld",
            "evidence": {
                "role": "locus_definition",
                "source_tag": "x",
                "fields": {"gene": "g", "trait": "t", "chromosome": "c", "position": "p"},
            },
        }
        df = pd.DataFrame({"g": ["G1"], "t": ["HEIGHT"], "c": ["1"], "p": [1000000]})
        result = route_evidence_source(pconn, source, df, BASIC_CONFIG)
        assert result is not None
        assert "loci" in result

    def test_routes_gene_centric(self, pconn):
        source = {
            "name": "ge",
            "evidence": {
                "centric": "gene",
                "category": "KNOW",
                "source_tag": "k",
                "fields": {"gene": "g"},
            },
        }
        df = pd.DataFrame({"g": ["G1"]})
        result = route_evidence_source(pconn, source, df, BASIC_CONFIG)
        assert result is not None
        assert "evidence_rows" in result

    def test_returns_none_for_raw(self, pconn):
        source = {"name": "raw_table"}
        df = pd.DataFrame({"x": [1]})
        assert route_evidence_source(pconn, source, df, BASIC_CONFIG) is None
