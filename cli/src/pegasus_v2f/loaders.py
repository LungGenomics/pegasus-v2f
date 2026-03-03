"""Data source loaders — all return pandas DataFrames."""

from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd

# Simple in-memory cache for XLSX downloads (avoids double-download during
# preview → load flow within a single CLI invocation).
_xlsx_cache: dict[str, bytes] = {}


def load_source(source: dict, data_dir: Path | None = None) -> pd.DataFrame:
    """Load a data source based on its source_type config.

    Args:
        source: Source config dict with at least 'source_type' and 'name'.
        data_dir: Base directory for resolving relative file paths (e.g., project_root/data/raw/).
    """
    source_type = source["source_type"]

    if source_type == "googlesheets":
        df = load_googlesheets(source)
    elif source_type == "file":
        df = load_file(source, data_dir)
    elif source_type == "excel":
        df = load_excel(source, data_dir)
    elif source_type == "url":
        df = load_url(source)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

    # Rename gene column if specified
    gene_col = source.get("gene_column")
    if gene_col and gene_col != "gene" and gene_col in df.columns:
        df = df.rename(columns={gene_col: "gene"})

    return df


def preview_source(source: dict, data_dir: Path | None = None, n_rows: int = 10) -> pd.DataFrame:
    """Fetch raw data with no skip applied, for previewing header rows.

    Returns a DataFrame with all rows as data (header=None), so the caller
    can display numbered rows and let the user pick which row is the header.
    """
    source_type = source["source_type"]

    if source_type == "googlesheets":
        xlsx_bytes = _fetch_googlesheets_xlsx(source)
        sheet = source.get("sheet", 0)
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, header=None,
                           nrows=n_rows, engine="calamine")
    elif source_type == "file":
        path = _resolve_path(source["path"], data_dir)
        df = pd.read_csv(path, sep=_guess_sep(path), header=None, nrows=n_rows)
    elif source_type == "excel":
        if "path" in source:
            path = _resolve_path(source["path"], data_dir)
        elif "url" in source:
            path = _download_to_cache(source["url"], source.get("cache"), data_dir)
        else:
            raise ValueError(f"Excel source needs 'path' or 'url'")
        sheet = source.get("sheet", 0)
        df = pd.read_excel(path, sheet_name=sheet, header=None, nrows=n_rows)
    elif source_type == "url":
        import httpx
        resp = httpx.get(source["url"], follow_redirects=True)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), sep=_guess_sep(source["url"]),
                         header=None, nrows=n_rows)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

    return df.head(n_rows)


def _fetch_googlesheets_xlsx(source: dict) -> bytes:
    """Download a Google Sheet as XLSX, with in-memory caching.

    Uses the /export?format=xlsx endpoint which preserves all data types
    and blank rows (unlike the gviz CSV endpoint which strips string values
    from columns it infers as numeric/boolean).
    """
    import httpx

    url = source["url"]
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if not match:
        raise ValueError(f"Could not extract spreadsheet ID from URL: {url}")
    spreadsheet_id = match.group(1)

    if spreadsheet_id in _xlsx_cache:
        return _xlsx_cache[spreadsheet_id]

    export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"
    resp = httpx.get(export_url, follow_redirects=True, timeout=120)
    resp.raise_for_status()

    _xlsx_cache[spreadsheet_id] = resp.content
    return resp.content


def load_googlesheets(source: dict) -> pd.DataFrame:
    """Load from Google Sheets URL (public sheets, no auth required).

    Downloads the spreadsheet as XLSX for accurate data — the gviz CSV endpoint
    strips column headers from numeric/boolean columns, losing data.
    Supports selecting a specific sheet tab by name via the ``sheet`` config key.
    """
    sheet = source.get("sheet", 0)
    skip_rows = source.get("skip_rows", 0)

    xlsx_bytes = _fetch_googlesheets_xlsx(source)
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=sheet, skiprows=skip_rows,
                       engine="calamine")
    return df


def load_file(source: dict, data_dir: Path | None = None) -> pd.DataFrame:
    """Load from local file (CSV, TSV, TSV.GZ)."""
    path = _resolve_path(source["path"], data_dir)
    return pd.read_csv(path, sep=_guess_sep(path))


def load_excel(source: dict, data_dir: Path | None = None) -> pd.DataFrame:
    """Load from Excel file (.xlsx)."""
    # Resolve path from config or download from URL
    if "path" in source:
        path = _resolve_path(source["path"], data_dir)
    elif "url" in source:
        path = _download_to_cache(source["url"], source.get("cache"), data_dir)
    else:
        raise ValueError(f"Excel source '{source.get('name')}' needs 'path' or 'url'")

    sheet = source.get("sheet", 0)
    skip_rows = source.get("skip_rows", 0)
    return pd.read_excel(path, sheet_name=sheet, skiprows=skip_rows)


def load_url(source: dict) -> pd.DataFrame:
    """Load from a remote URL (CSV/TSV)."""
    import httpx

    url = source["url"]
    resp = httpx.get(url, follow_redirects=True)
    resp.raise_for_status()

    # Write to temp file and read with pandas
    import tempfile
    suffix = ".csv" if ".csv" in url else ".tsv"
    with tempfile.NamedTemporaryFile(suffix=suffix, mode="w", delete=False) as f:
        f.write(resp.text)
        tmp_path = f.name

    try:
        return pd.read_csv(tmp_path, sep=_guess_sep(tmp_path))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _resolve_path(path: str, data_dir: Path | None) -> Path:
    """Resolve a file path, optionally relative to data_dir."""
    p = Path(path)
    if p.is_absolute():
        return p
    if data_dir:
        resolved = data_dir / p
        if resolved.exists():
            return resolved
    # Try relative to cwd
    return Path(path)


def _guess_sep(path: str | Path) -> str:
    """Guess delimiter from file extension."""
    path_str = str(path).lower()
    if ".tsv" in path_str:
        return "\t"
    return ","


def _download_to_cache(url: str, cache_dir: str | None, data_dir: Path | None) -> Path:
    """Download a URL to a local cache directory."""
    import httpx

    # Determine cache location
    if cache_dir and data_dir:
        dest_dir = data_dir / cache_dir
    elif data_dir:
        dest_dir = data_dir / "cache"
    else:
        dest_dir = Path(".v2f") / "cache"

    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1].split("?")[0]
    dest = dest_dir / filename

    if not dest.exists():
        resp = httpx.get(url, follow_redirects=True)
        resp.raise_for_status()
        dest.write_bytes(resp.content)

    return dest
