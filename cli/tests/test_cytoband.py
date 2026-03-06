"""Tests for cytoband lookup utility."""

from pathlib import Path
from unittest.mock import patch

import pytest

from pegasus_v2f.cytoband import (
    clear_cache,
    get_cytoband,
    get_cytoband_for_region,
    _normalize_chrom,
)


# Minimal fixture data mimicking UCSC cytoBand.txt format
FIXTURE_DATA = """\
chr1\t0\t2300000\tp36.33\tgneg
chr1\t2300000\t5400000\tp36.32\tgpos25
chr1\t5400000\t7200000\tp36.31\tgneg
chr1\t7200000\t9200000\tp36.23\tgpos25
chr2\t0\t3300000\tp25.3\tgneg
chr2\t3300000\t4500000\tp25.2\tgpos25
"""


@pytest.fixture(autouse=True)
def _clear_module_cache():
    """Clear the module-level band cache before each test."""
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def cache_dir(tmp_path):
    """Create a cache directory with fixture cytoband data."""
    d = tmp_path / ".v2f"
    d.mkdir()
    (d / "cytoBand.txt").write_text(FIXTURE_DATA)
    return d


class TestNormalizeChrom:
    def test_strips_chr_prefix(self):
        assert _normalize_chrom("chr1") == "1"

    def test_no_prefix(self):
        assert _normalize_chrom("1") == "1"

    def test_strips_whitespace(self):
        assert _normalize_chrom(" chr2 ") == "2"


class TestGetCytoband:
    def test_known_position(self, cache_dir):
        assert get_cytoband("1", 1000000, cache_dir) == "1p36.33"

    def test_second_band(self, cache_dir):
        assert get_cytoband("1", 3000000, cache_dir) == "1p36.32"

    def test_different_chromosome(self, cache_dir):
        assert get_cytoband("2", 1000000, cache_dir) == "2p25.3"

    def test_chr_prefix_input(self, cache_dir):
        assert get_cytoband("chr1", 1000000, cache_dir) == "1p36.33"

    def test_position_not_found_fallback(self, cache_dir):
        # Position way beyond our fixture data
        result = get_cytoband("1", 999999999, cache_dir)
        assert result == "chr1_999999999"

    def test_unknown_chromosome_fallback(self, cache_dir):
        result = get_cytoband("99", 1000, cache_dir)
        assert result == "chr99_1000"


class TestGetCytobandForRegion:
    def test_single_band(self, cache_dir):
        result = get_cytoband_for_region("1", 100000, 2000000, cache_dir)
        assert result == "1p36.33"

    def test_spanning_bands(self, cache_dir):
        # Spans from p36.33 into p36.32
        result = get_cytoband_for_region("1", 100000, 3000000, cache_dir)
        assert result == "1p36.33-p36.32"

    def test_spanning_multiple_bands(self, cache_dir):
        # Spans p36.33, p36.32, p36.31
        result = get_cytoband_for_region("1", 100000, 6000000, cache_dir)
        assert result == "1p36.33-p36.31"

    def test_no_bands_found_fallback(self, cache_dir):
        result = get_cytoband_for_region("99", 100, 200, cache_dir)
        assert result == "chr99_100"


class TestDownload:
    def test_downloads_on_first_use(self, tmp_path):
        """Test that download is triggered when cache file is missing."""
        d = tmp_path / ".v2f"
        d.mkdir()

        # Mock the download to write fixture data instead
        def mock_download(cache_dir, cache_file):
            cache_file.write_text(FIXTURE_DATA)

        with patch("pegasus_v2f.cytoband._download", side_effect=mock_download):
            result = get_cytoband("1", 1000000, d)
            assert result == "1p36.33"
