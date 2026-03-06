"""Database introspection, config, and status endpoints."""

from __future__ import annotations

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pegasus_v2f.db import is_postgres
from pegasus_v2f_api.db_helpers import clean_rows, execute_query, get_stored_config, has_table

# Hardcoded seqcol digest mapping for chromosome sizes
SEQCOL_DIGESTS: dict[str, str] = {
    "hg38": "NTeQ1GQMt2ocCFkS8Z3_qkvetZjabWSt",
    "GRCh38": "NTeQ1GQMt2ocCFkS8Z3_qkvetZjabWSt",
}

SEQCOL_API = "https://seqcolapi.databio.org"

STANDARD_CHROMS = {f"chr{i}" for i in range(1, 23)} | {"chrX", "chrY"}

# Module-level cache for chrom sizes (genome_build -> result)
_chrom_sizes_cache: dict[str, dict] = {}

router = APIRouter()


# --- Request models ---

class QueryRequest(BaseModel):
    query: str


class DeleteTableRequest(BaseModel):
    table_name: str


class MetaUpdateRequest(BaseModel):
    key: str
    value: str


class ReconnectRequest(BaseModel):
    db: str


# --- Raw query ---

@router.post("/db/query")
async def db_query(req: QueryRequest, request: Request):
    """Execute an SQL query and return results."""
    conn = request.app.state.conn
    try:
        if is_postgres(conn):
            import psycopg2.extras

            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(req.query)
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        else:
            columns, rows = execute_query(conn, req.query)
            return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Tables ---

@router.get("/db/tables")
async def list_tables(request: Request):
    """List all tables with row counts."""
    from pegasus_v2f.db_schema import list_tables as _list_tables

    conn = request.app.state.conn
    return _list_tables(conn)


# --- Config ---

@router.get("/db/config")
async def get_config(request: Request):
    """Read stored config from database."""
    conn = request.app.state.conn
    return get_stored_config(conn)


# --- Status ---

@router.get("/db/status")
async def db_status(request: Request):
    """Dashboard stats: counts for studies, loci, genes, evidence, sources."""
    conn = request.app.state.conn

    def _count(table: str) -> int:
        if not has_table(conn, table):
            return 0
        try:
            _, rows = execute_query(conn, f"SELECT COUNT(*) FROM {table}")
            return rows[0][0]
        except Exception:
            return 0

    # Metadata from _pegasus_meta
    from pegasus_v2f.db_meta import read_meta

    status = {
        "n_studies": _count("studies"),
        "n_loci": _count("loci"),
        "n_genes": _count("genes"),
        "n_evidence_rows": _count("evidence"),
        "n_scored_rows": _count("scored_evidence"),
        "n_sources": _count("data_sources"),
        "has_pegasus": has_table(conn, "scored_evidence"),
        "genome_build": read_meta(conn, "genome_build") or "-",
        "package_version": read_meta(conn, "package_version") or "-",
    }
    return status


# --- Evidence categories ---

@router.get("/db/evidence-categories")
async def evidence_categories():
    """Return PEGASUS evidence category vocabulary."""
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES

    return EVIDENCE_CATEGORIES


# --- Chromosome sizes ---

@router.get("/db/chrom-sizes")
async def chrom_sizes(request: Request):
    """Return chromosome names and lengths for the database genome build.

    Fetches from the seqcol API and caches the result. Returns only standard
    chromosomes (chr1-22, X, Y) in karyotype order.
    """
    from pegasus_v2f.db_meta import read_meta

    conn = request.app.state.conn
    genome_build = read_meta(conn, "genome_build") or "hg38"

    # Check cache
    if genome_build in _chrom_sizes_cache:
        return _chrom_sizes_cache[genome_build]

    digest = SEQCOL_DIGESTS.get(genome_build)
    if not digest:
        raise HTTPException(
            status_code=400,
            detail=f"No seqcol digest known for genome build '{genome_build}'",
        )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{SEQCOL_API}/collection/{digest}?level=2")
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch from seqcol API: {e}")

    all_names = data["names"]
    all_lengths = data["lengths"]

    # Filter to standard chroms and sort in karyotype order
    chrom_order = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]
    names = []
    lengths = []
    lookup = dict(zip(all_names, all_lengths))
    for chrom in chrom_order:
        if chrom in lookup:
            names.append(chrom)
            lengths.append(lookup[chrom])

    result = {"names": names, "lengths": lengths}
    _chrom_sizes_cache[genome_build] = result
    return result


# --- Meta update ---

@router.patch("/db/meta")
async def update_meta(req: MetaUpdateRequest, request: Request):
    """Update a single _pegasus_meta key-value pair."""
    from pegasus_v2f.db_meta import write_meta

    conn = request.app.state.conn
    try:
        write_meta(conn, req.key, req.value)
        return {"success": True, "key": req.key, "value": req.value}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Reconnect ---

@router.post("/db/reconnect")
async def reconnect_db(req: ReconnectRequest, request: Request):
    """Close current connection and open a new database."""
    from pegasus_v2f.db import get_connection

    try:
        old_conn = request.app.state.conn
        try:
            old_conn.close()
        except Exception:
            pass

        new_conn = get_connection(db=req.db)
        request.app.state.conn = new_conn

        # Return new status
        return await db_status(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Backward compatibility aliases ---

@router.get("/tables")
async def list_tables_compat(request: Request):
    """Alias for /db/tables (backward compat)."""
    return await list_tables(request)


@router.get("/config")
async def get_config_compat(request: Request):
    """Alias for /db/config (backward compat)."""
    return await get_config(request)


@router.post("/db/delete_table")
async def delete_table_compat(req: DeleteTableRequest, request: Request):
    """Alias for DELETE /sources/{name} (backward compat)."""
    from pegasus_v2f.sources import remove_source

    conn = request.app.state.conn
    config = get_stored_config(conn)
    try:
        remove_source(conn, req.table_name, config=config)
        return {"success": True, "deleted": req.table_name}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/db/update_metadata")
async def update_metadata_compat(request: Request):
    """Alias for PATCH /sources/{name}/metadata (backward compat)."""
    from pegasus_v2f_api.routes.sources import UpdateMetadataRequest

    body = await request.json()
    req = UpdateMetadataRequest(**body)
    conn = request.app.state.conn
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                """UPDATE source_metadata SET
                    description = %s, display_name = %s, data_type = %s
                WHERE table_name = %s""",
                (req.description, req.display_name, req.data_type, req.table_name),
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                """UPDATE source_metadata SET
                    description = ?, display_name = ?, data_type = ?
                WHERE table_name = ?""",
                [req.description, req.display_name, req.data_type, req.table_name],
            )
        return {"success": True, "updated": req.table_name}
    except Exception as e:
        return {"success": False, "error": str(e)}
