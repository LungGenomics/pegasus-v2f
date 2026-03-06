"""Source management endpoints — list, import, preview, update, delete."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pegasus_v2f.db import is_postgres
from pegasus_v2f_api.db_helpers import clean_rows, execute_query, get_stored_config, has_table

router = APIRouter()


# --- Request models ---

class ImportDataRequest(BaseModel):
    name: str = "new_table"
    data: list[dict[str, Any]]
    description: str = ""
    display_name: str = ""
    data_type: str = "custom"
    source_type: str = "googlesheets"
    gene_column: str = "gene"
    include_in_search: bool = False
    url: str = ""
    sheet: str = ""
    skip_rows: int = 0


class FetchGoogleRequest(BaseModel):
    ss: str
    sheet: str = ""
    skip: int = 0


class UpdateMetadataRequest(BaseModel):
    table_name: str
    description: str = ""
    display_name: str = ""
    data_type: str = ""


# --- List sources ---

@router.get("/sources")
async def list_sources(request: Request):
    """List data sources from stored config."""
    from pegasus_v2f.sources import list_sources as _list_sources

    conn = request.app.state.conn
    return _list_sources(conn)


# --- Source provenance ---

@router.get("/sources/provenance")
async def source_provenance(request: Request):
    """PEGASUS data source provenance from the data_sources table."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "data_sources"):
            return []

        columns, rows = execute_query(
            conn,
            "SELECT source_tag, source_name, source_type, evidence_category, "
            "is_integrated, version, url, citation, date_imported, record_count "
            "FROM data_sources ORDER BY evidence_category, source_tag",
        )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Preview (fetch google sheet) ---

@router.post("/sources/preview")
async def preview_source(req: FetchGoogleRequest):
    """Fetch data from a Google Sheet (preview only)."""
    try:
        from pegasus_v2f.loaders import load_source

        source: dict[str, Any] = {
            "name": "_preview",
            "source_type": "googlesheets",
            "url": req.ss,
        }
        if req.sheet:
            source["sheet"] = req.sheet
        if req.skip:
            source["skip_rows"] = req.skip

        df = load_source(source)
        return df.head(100).to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Import ---

@router.post("/sources/import")
async def import_data(req: ImportDataRequest, request: Request):
    """Import data into the database."""
    from pegasus_v2f.sources import add_source

    conn = request.app.state.conn
    config = get_stored_config(conn)
    try:
        source: dict[str, Any] = {
            "name": req.name,
            "source_type": req.source_type,
            "display_name": req.display_name or req.name,
            "description": req.description,
            "data_type": req.data_type,
            "gene_column": req.gene_column,
            "include_in_search": req.include_in_search,
        }
        if req.url:
            source["url"] = req.url

        rows = add_source(conn, source, config=config)
        return {"success": True, "imported": req.name, "rows": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Update source ---

@router.post("/sources/{name}/update")
async def update_source(name: str, request: Request):
    """Re-fetch and reload an existing source."""
    from pegasus_v2f.sources import update_source as _update_source

    conn = request.app.state.conn
    config = get_stored_config(conn)
    try:
        rows = _update_source(conn, name, config=config)
        return {"success": True, "updated": name, "rows": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Delete source ---

@router.delete("/sources/{name}")
async def delete_source(name: str, request: Request):
    """Delete a source and remove from config."""
    from pegasus_v2f.sources import remove_source

    conn = request.app.state.conn
    config = get_stored_config(conn)
    try:
        remove_source(conn, name, config=config)
        return {"success": True, "deleted": name}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Update metadata ---

@router.patch("/sources/{name}/metadata")
async def update_metadata(name: str, req: UpdateMetadataRequest, request: Request):
    """Update metadata for a data source."""
    conn = request.app.state.conn
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                """UPDATE source_metadata SET
                    description = %s, display_name = %s, data_type = %s
                WHERE table_name = %s""",
                (req.description, req.display_name, req.data_type, name),
            )
            conn.commit()
            cur.close()
        else:
            conn.execute(
                """UPDATE source_metadata SET
                    description = ?, display_name = ?, data_type = ?
                WHERE table_name = ?""",
                [req.description, req.display_name, req.data_type, name],
            )
        return {"success": True, "updated": name}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Materialize scores ---

@router.post("/sources/materialize")
async def materialize_scores(request: Request):
    """Re-run evidence scoring (materialize_scored_evidence)."""
    from pegasus_v2f.scoring import materialize_scored_evidence

    conn = request.app.state.conn
    config = get_stored_config(conn)
    try:
        if not config.get("pegasus"):
            return {"success": False, "error": "No pegasus config — scoring requires a PEGASUS database"}
        rows = materialize_scored_evidence(conn, config)
        return {"success": True, "scored": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Backward compatibility aliases ---

@router.post("/import/fetch_google")
async def fetch_google_compat(req: FetchGoogleRequest):
    """Alias for /sources/preview (backward compat)."""
    return await preview_source(req)


@router.post("/import/import_data")
async def import_data_compat(req: ImportDataRequest, request: Request):
    """Alias for /sources/import (backward compat)."""
    return await import_data(req, request)
