"""Cytoband lookup — download and cache UCSC cytoBand data for GRCh38."""

from __future__ import annotations

import gzip
import logging
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

CYTOBAND_URL = "https://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/cytoBand.txt.gz"
CACHE_FILENAME = "cytoBand.txt"

# Module-level cache: list of (chrom, start, end, band_name)
_bands: list[tuple[str, int, int, str]] | None = None


def _normalize_chrom(chrom: str) -> str:
    """Strip 'chr' prefix for consistent comparison."""
    s = str(chrom).strip()
    return s[3:] if s.startswith("chr") else s


def _load_bands(cache_dir: Path) -> list[tuple[str, int, int, str]]:
    """Load cytoband data from cache, downloading if needed."""
    global _bands
    if _bands is not None:
        return _bands

    cache_file = cache_dir / CACHE_FILENAME
    if not cache_file.exists():
        _download(cache_dir, cache_file)

    bands = []
    for line in cache_file.read_text().splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        chrom = _normalize_chrom(parts[0])
        start = int(parts[1])
        end = int(parts[2])
        name = parts[3]
        bands.append((chrom, start, end, name))

    _bands = bands
    return _bands


def _download(cache_dir: Path, cache_file: Path) -> None:
    """Download and gunzip cytoBand.txt.gz from UCSC."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading cytoBand data from {CYTOBAND_URL}")
    gz_path = cache_file.with_suffix(".txt.gz")
    urllib.request.urlretrieve(CYTOBAND_URL, gz_path)

    with gzip.open(gz_path, "rt") as f_in:
        cache_file.write_text(f_in.read())

    gz_path.unlink()
    logger.info(f"Cached cytoBand data at {cache_file}")


def get_cytoband(chromosome: str, position: int, cache_dir: Path) -> str:
    """Return cytoband for a genomic position (e.g. '1p36.33').

    Downloads hg38 cytoBand.txt.gz on first call, caches in cache_dir.
    """
    bands = _load_bands(cache_dir)
    chrom = _normalize_chrom(chromosome)

    for b_chrom, b_start, b_end, b_name in bands:
        if b_chrom == chrom and b_start <= position < b_end:
            return f"{chrom}{b_name}"

    return f"chr{chrom}_{position}"


def get_cytoband_for_region(
    chromosome: str, start: int, end: int, cache_dir: Path
) -> str:
    """Return cytoband(s) spanning a region.

    If start and end are in the same band: '1p36.33'
    If they span bands: '1p36.33-p36.31'
    """
    bands = _load_bands(cache_dir)
    chrom = _normalize_chrom(chromosome)

    # Find all bands overlapping the region
    overlapping = []
    for b_chrom, b_start, b_end, b_name in bands:
        if b_chrom == chrom and b_start < end and b_end > start:
            overlapping.append(b_name)

    if not overlapping:
        return f"chr{chrom}_{start}"

    if len(overlapping) == 1 or overlapping[0] == overlapping[-1]:
        return f"{chrom}{overlapping[0]}"

    return f"{chrom}{overlapping[0]}-{overlapping[-1]}"


def clear_cache() -> None:
    """Clear the module-level band cache (useful for testing)."""
    global _bands
    _bands = None
