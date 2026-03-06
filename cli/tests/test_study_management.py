"""Tests for study management — add_study, preview_study, sentinel clustering."""

import duckdb
import pandas as pd
import pytest

from pegasus_v2f.pegasus_schema import create_pegasus_schema
from pegasus_v2f.study_management import (
    add_study,
    preview_study,
    _cluster_sentinels,
    _merge_windows,
)


@pytest.fixture
def conn():
    """In-memory DuckDB with PEGASUS schema."""
    c = duckdb.connect(":memory:")
    create_pegasus_schema(c)
    yield c
    c.close()


@pytest.fixture
def sentinel_df():
    """Basic sentinel DataFrame with 3 variants on chr1."""
    return pd.DataFrame({
        "chromosome": ["1", "1", "1"],
        "position": [1_000_000, 1_200_000, 5_000_000],
        "rsid": ["rs1", "rs2", "rs3"],
        "pvalue": [1e-10, 1e-8, 1e-12],
    })


@pytest.fixture
def multi_trait_sentinel_df():
    """Sentinel DataFrame with trait column."""
    return pd.DataFrame({
        "chromosome": ["1", "1", "2"],
        "position": [1_000_000, 5_000_000, 3_000_000],
        "rsid": ["rs1", "rs2", "rs3"],
        "pvalue": [1e-10, 1e-8, 1e-12],
        "trait": ["HEIGHT", "HEIGHT", "FVC"],
    })


class TestAddStudy:
    def test_creates_study_and_loci(self, conn, sentinel_df):
        result = add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        assert result["study_name"] == "test_study"
        assert result["n_sentinels"] == 3
        assert result["n_loci"] > 0
        assert len(result["study_ids"]) == 1
        assert result["study_ids"][0] == "test_study_height"

        # Check study row
        rows = conn.execute("SELECT * FROM studies").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test_study_height"  # study_id
        assert rows[0][1] == "test_study"  # study_name
        assert rows[0][2] == "HEIGHT"  # trait

        # Check loci exist
        loci = conn.execute("SELECT * FROM loci").fetchall()
        assert len(loci) > 0

    def test_stores_raw_sentinel_table(self, conn, sentinel_df):
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        rows = conn.execute("SELECT * FROM loci_test_study").fetchall()
        assert len(rows) == 3

    def test_multi_trait_creates_multiple_studies(self, conn, multi_trait_sentinel_df):
        result = add_study(
            conn, "test_study", ["HEIGHT", "FVC"],
            loci_df=multi_trait_sentinel_df,
        )

        assert len(result["study_ids"]) == 2
        assert "test_study_height" in result["study_ids"]
        assert "test_study_fvc" in result["study_ids"]

        # Two study rows
        rows = conn.execute("SELECT * FROM studies ORDER BY trait").fetchall()
        assert len(rows) == 2

    def test_trait_column_splits_sentinels(self, conn, multi_trait_sentinel_df):
        result = add_study(
            conn, "test_study", ["HEIGHT", "FVC"],
            loci_df=multi_trait_sentinel_df,
        )

        # HEIGHT gets 2 sentinels, FVC gets 1
        height_loci = conn.execute(
            "SELECT * FROM loci WHERE study_id = 'test_study_height'"
        ).fetchall()
        fvc_loci = conn.execute(
            "SELECT * FROM loci WHERE study_id = 'test_study_fvc'"
        ).fetchall()
        assert len(height_loci) >= 1
        assert len(fvc_loci) == 1

    def test_no_trait_column_uses_all_sentinels_for_each_trait(self, conn, sentinel_df):
        result = add_study(
            conn, "test_study", ["HEIGHT", "FVC"],
            loci_df=sentinel_df,
        )

        # Both traits get all sentinels → same loci
        height_loci = conn.execute(
            "SELECT COUNT(*) FROM loci WHERE study_id = 'test_study_height'"
        ).fetchone()[0]
        fvc_loci = conn.execute(
            "SELECT COUNT(*) FROM loci WHERE study_id = 'test_study_fvc'"
        ).fetchone()[0]
        assert height_loci == fvc_loci

    def test_n_loci_updated_on_study(self, conn, sentinel_df):
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        n_loci = conn.execute(
            "SELECT n_loci FROM studies WHERE study_id = 'test_study_height'"
        ).fetchone()[0]
        actual = conn.execute(
            "SELECT COUNT(*) FROM loci WHERE study_id = 'test_study_height'"
        ).fetchone()[0]
        assert n_loci == actual

    def test_optional_metadata(self, conn, sentinel_df):
        add_study(
            conn, "test_study", ["HEIGHT"],
            loci_df=sentinel_df,
            gwas_source="PMID:12345",
            ancestry="European",
            sex="both",
            sample_size=100000,
            doi="10.1234/test",
            year=2024,
        )

        row = conn.execute(
            "SELECT gwas_source, ancestry, sex, sample_size, doi, year "
            "FROM studies WHERE study_id = 'test_study_height'"
        ).fetchone()
        assert row[0] == "PMID:12345"
        assert row[1] == "European"
        assert row[2] == "both"
        assert row[3] == 100000
        assert row[4] == "10.1234/test"
        assert row[5] == 2024

    def test_duplicate_traits_rejected(self, conn, sentinel_df):
        with pytest.raises(ValueError, match="Duplicate traits"):
            add_study(conn, "test_study", ["HEIGHT", "HEIGHT"], loci_df=sentinel_df)

    def test_no_traits_rejected(self, conn, sentinel_df):
        with pytest.raises(ValueError, match="At least one trait"):
            add_study(conn, "test_study", [], loci_df=sentinel_df)

    def test_chr_pos_column_aliases(self, conn):
        """'chr' and 'pos' column names are accepted."""
        df = pd.DataFrame({
            "chr": ["1", "1"],
            "pos": [1_000_000, 5_000_000],
        })
        result = add_study(conn, "test_study", ["HEIGHT"], loci_df=df)
        assert result["n_loci"] > 0


class TestClusterSentinels:
    def test_nearby_sentinels_merge(self):
        """Two sentinels within window+merge should form one locus."""
        df = pd.DataFrame({
            "chromosome": ["1", "1"],
            "position": [1_000_000, 1_200_000],
        })
        loci = _cluster_sentinels(df, window_kb=500, merge_distance_kb=250)
        assert len(loci) == 1
        assert len(loci[0]["sentinels"]) == 2

    def test_distant_sentinels_separate(self):
        """Two far-apart sentinels should form two loci."""
        df = pd.DataFrame({
            "chromosome": ["1", "1"],
            "position": [1_000_000, 10_000_000],
        })
        loci = _cluster_sentinels(df, window_kb=500, merge_distance_kb=250)
        assert len(loci) == 2

    def test_different_chromosomes_separate(self):
        """Sentinels on different chromosomes never merge."""
        df = pd.DataFrame({
            "chromosome": ["1", "2"],
            "position": [1_000_000, 1_000_000],
        })
        loci = _cluster_sentinels(df, window_kb=500, merge_distance_kb=250)
        assert len(loci) == 2

    def test_window_size_affects_merging(self):
        """Larger window merges sentinels that smaller window separates."""
        df = pd.DataFrame({
            "chromosome": ["1", "1"],
            "position": [1_000_000, 3_000_000],
        })
        small = _cluster_sentinels(df, window_kb=100, merge_distance_kb=0)
        large = _cluster_sentinels(df, window_kb=1500, merge_distance_kb=0)
        assert len(small) == 2
        assert len(large) == 1

    def test_locus_window_boundaries(self):
        """Locus start/end reflect the window around sentinel position."""
        df = pd.DataFrame({
            "chromosome": ["1"],
            "position": [1_000_000],
        })
        loci = _cluster_sentinels(df, window_kb=500, merge_distance_kb=0)
        assert len(loci) == 1
        assert loci[0]["start"] == 500_000
        assert loci[0]["end"] == 1_500_000


class TestPreviewStudy:
    def test_preview_with_evidence(self, conn, sentinel_df):
        """Preview returns locus summaries with evidence counts."""
        # Set up study + loci
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        # Add a gene overlapping locus
        conn.execute(
            "INSERT INTO genes (gene_symbol, chromosome, start_position, end_position) "
            "VALUES ('GENE_A', '1', 950000, 1050000)"
        )
        # Add gene-level evidence
        conn.execute(
            "INSERT INTO evidence (gene_symbol, evidence_category, source_tag) "
            "VALUES ('GENE_A', 'GWAS', 'src1')"
        )

        results = preview_study(conn, "test_study")
        assert len(results) > 0
        # At least one locus should have evidence
        has_evidence = any(r["n_evidence_rows"] > 0 for r in results)
        assert has_evidence

    def test_preview_no_evidence(self, conn, sentinel_df):
        """Preview works when no evidence exists."""
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        results = preview_study(conn, "test_study")
        assert len(results) > 0
        # All loci should have 0 evidence
        assert all(r["n_evidence_rows"] == 0 for r in results)

    def test_preview_nonexistent_study(self, conn):
        """Preview returns empty for nonexistent study."""
        results = preview_study(conn, "nonexistent")
        assert results == []


class TestLocusFields:
    """Test cytoband naming, n_signals, and locus_source."""

    def test_n_signals_from_sentinel_count(self, conn, sentinel_df):
        """n_signals reflects number of sentinels merged into each locus."""
        # With default window/merge, rs1 and rs2 (200kb apart) should merge
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df,
                  window_kb=500, merge_distance_kb=250)

        rows = conn.execute(
            "SELECT n_signals FROM loci WHERE study_id = 'test_study_height' ORDER BY start_position"
        ).fetchall()
        # First locus merges rs1+rs2 (200kb apart, within 500kb window)
        assert rows[0][0] == 2
        # Second locus is rs3 alone (5M, far away)
        assert rows[1][0] == 1

    def test_locus_source_is_sentinel_clustering(self, conn, sentinel_df):
        """locus_source is 'sentinel_clustering' for auto-clustered loci."""
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        sources = conn.execute(
            "SELECT DISTINCT locus_source FROM loci"
        ).fetchall()
        assert len(sources) == 1
        assert sources[0][0] == "sentinel_clustering"

    def test_cytoband_naming_with_cache(self, conn, sentinel_df, tmp_path):
        """Locus names use cytobands when cache_dir is provided."""
        from unittest.mock import patch

        def mock_cytoband(chrom, start, end, cache_dir):
            return f"{chrom}p36.33"

        with patch("pegasus_v2f.cytoband.get_cytoband_for_region",
                    side_effect=mock_cytoband):
            add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df,
                      cache_dir=tmp_path)

        names = conn.execute(
            "SELECT locus_name FROM loci ORDER BY start_position"
        ).fetchall()
        assert all("p36.33" in n[0] for n in names)

    def test_fallback_naming_without_cache(self, conn, sentinel_df):
        """Without cache_dir, locus names fall back to chr:pos format."""
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        names = conn.execute(
            "SELECT locus_name FROM loci ORDER BY start_position"
        ).fetchall()
        # Should be chr:start-end format
        assert all(n[0].startswith("chr1:") for n in names)


class TestConfigSync:
    """Test v2f.yaml sync from add_study (Gap 7)."""

    def test_config_path_writes_yaml(self, conn, sentinel_df, tmp_path):
        """add_study with config_path writes study to v2f.yaml."""
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\n")

        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df,
                  config_path=config_path, gwas_source="PMID:12345")

        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)

        studies = config["pegasus"]["study"]
        assert len(studies) == 1
        assert studies[0]["id_prefix"] == "test_study"
        assert studies[0]["traits"] == ["HEIGHT"]
        assert studies[0]["gwas_source"] == "PMID:12345"

    def test_no_config_path_skips_yaml(self, conn, sentinel_df):
        """add_study without config_path only writes to _pegasus_meta."""
        # Should not raise even with no config_path
        add_study(conn, "test_study", ["HEIGHT"], loci_df=sentinel_df)

        # Verify _pegasus_meta was written
        from pegasus_v2f.db_meta import read_meta
        import yaml
        meta = yaml.safe_load(read_meta(conn, "config"))
        studies = meta["pegasus"]["study"]
        assert any(s["id_prefix"] == "test_study" for s in studies)


class TestSentinelFile:
    def test_reads_tsv(self, tmp_path):
        from pegasus_v2f.study_management import _read_sentinel_file

        path = tmp_path / "sentinels.tsv"
        path.write_text("chromosome\tposition\trsid\n1\t1000000\trs1\n")
        df = _read_sentinel_file(path)
        assert len(df) == 1
        assert "chromosome" in df.columns

    def test_reads_csv(self, tmp_path):
        from pegasus_v2f.study_management import _read_sentinel_file

        path = tmp_path / "sentinels.csv"
        path.write_text("chromosome,position,rsid\n1,1000000,rs1\n")
        df = _read_sentinel_file(path)
        assert len(df) == 1

    def test_missing_file_raises(self, tmp_path):
        from pegasus_v2f.study_management import _read_sentinel_file

        with pytest.raises(FileNotFoundError):
            _read_sentinel_file(tmp_path / "nope.tsv")
