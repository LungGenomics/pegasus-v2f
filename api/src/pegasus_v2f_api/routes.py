"""API route handlers — ported from old Plumber API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pegasus_v2f.db import is_postgres

router = APIRouter()


# --- Request models ---

class QueryRequest(BaseModel):
    query: str


class UpdateMetadataRequest(BaseModel):
    table_name: str
    description: str = ""
    display_name: str = ""
    data_type: str = ""


class DeleteTableRequest(BaseModel):
    table_name: str


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


# --- Database query ---

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
            result = conn.execute(req.query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [
                {col: _clean_value(val) for col, val in zip(columns, row)}
                for row in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Tables ---

@router.get("/tables")
async def list_tables(request: Request):
    """List all tables with row counts."""
    from pegasus_v2f.db_schema import list_tables as _list_tables
    conn = request.app.state.conn
    return _list_tables(conn)


# --- Sources ---

@router.get("/sources")
async def list_sources(request: Request):
    """List data sources from stored config."""
    from pegasus_v2f.sources import list_sources as _list_sources
    conn = request.app.state.conn
    return _list_sources(conn)


# --- Config ---

@router.get("/config")
async def get_config(request: Request):
    """Read stored config from database."""
    import yaml
    from pegasus_v2f.db_meta import read_meta
    conn = request.app.state.conn
    config_yaml = read_meta(conn, "config")
    if not config_yaml:
        return {}
    return yaml.safe_load(config_yaml)


# --- Gene search ---

@router.get("/genes")
async def search_genes(request: Request, search: str = "", limit: int = 100):
    """Search genes by name or symbol."""
    conn = request.app.state.conn

    if not search:
        # Return all genes (paginated)
        try:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(
                    "SELECT * FROM gene_search_index ORDER BY gene LIMIT %s", (limit,)
                )
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                cur.close()
            else:
                result = conn.execute(
                    "SELECT * FROM gene_search_index ORDER BY gene LIMIT ?", [limit]
                )
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
            return [
                {col: _clean_value(val) for col, val in zip(columns, row)}
                for row in rows
            ]
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    # Search using DuckDB FTS or Postgres LIKE
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM gene_search_index WHERE gene ILIKE %s ORDER BY gene LIMIT %s",
                (f"%{search}%", limit),
            )
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()
        else:
            # Try FTS first, fall back to LIKE
            try:
                result = conn.execute(
                    """SELECT gsi.*, fts.score
                    FROM fts_main_gene_search_index fts
                    JOIN gene_search_index gsi ON gsi.ensembl_gene_id = fts.ensembl_gene_id
                    WHERE fts.match_bm25(ensembl_gene_id, ?) IS NOT NULL
                    ORDER BY fts.score DESC LIMIT ?""",
                    [search, limit],
                )
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
            except Exception:
                result = conn.execute(
                    "SELECT * FROM gene_search_index WHERE gene LIKE ? OR searchable_text LIKE ? ORDER BY gene LIMIT ?",
                    [f"%{search}%", f"%{search}%", limit],
                )
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()

        return [
            {col: _clean_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Metadata update ---

@router.post("/db/update_metadata")
async def update_metadata(req: UpdateMetadataRequest, request: Request):
    """Update metadata for a data source."""
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


# --- Delete table ---

@router.post("/db/delete_table")
async def delete_table(req: DeleteTableRequest, request: Request):
    """Delete a table and remove from config."""
    from pegasus_v2f.sources import remove_source
    conn = request.app.state.conn
    try:
        remove_source(conn, req.table_name)
        return {"success": True, "deleted": req.table_name}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- Import ---

@router.post("/import/fetch_google")
async def fetch_google(req: FetchGoogleRequest):
    """Fetch data from a Google Sheet (preview only)."""
    try:
        from pegasus_v2f.loaders import load_source
        source = {
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


@router.post("/import/import_data")
async def import_data(req: ImportDataRequest, request: Request):
    """Import data into the database."""
    import pandas as pd
    from pegasus_v2f.sources import add_source

    conn = request.app.state.conn
    try:
        source = {
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

        rows = add_source(conn, source)
        return {"success": True, "imported": req.name, "rows": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}


# --- PEGASUS evidence endpoints ---

@router.get("/evidence/{gene}")
async def get_gene_evidence(gene: str, request: Request):
    """Get all evidence for a gene (locus-level + gene-level)."""
    conn = request.app.state.conn
    try:
        rows = []
        # Locus-gene evidence
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT locus_id, gene_symbol, evidence_category, evidence_stream, "
                "source_tag, pvalue, effect_size, score, tissue, cell_type "
                "FROM locus_gene_evidence WHERE gene_symbol = %s",
                (gene,),
            )
            cols = [desc[0] for desc in cur.description]
            for r in cur.fetchall():
                d = {c: _clean_value(v) for c, v in zip(cols, r)}
                d["evidence_level"] = "locus"
                rows.append(d)
            cur.close()
        else:
            result = conn.execute(
                "SELECT locus_id, gene_symbol, evidence_category, evidence_stream, "
                "source_tag, pvalue, effect_size, score, tissue, cell_type "
                "FROM locus_gene_evidence WHERE gene_symbol = ?",
                [gene],
            )
            cols = [desc[0] for desc in result.description]
            for r in result.fetchall():
                d = {c: _clean_value(v) for c, v in zip(cols, r)}
                d["evidence_level"] = "locus"
                rows.append(d)

        # Gene-level evidence
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT gene_symbol, evidence_category, evidence_type, "
                "source_tag, score, tissue, cell_type "
                "FROM gene_evidence WHERE gene_symbol = %s",
                (gene,),
            )
            cols = [desc[0] for desc in cur.description]
            for r in cur.fetchall():
                d = {c: _clean_value(v) for c, v in zip(cols, r)}
                d["evidence_level"] = "gene"
                rows.append(d)
            cur.close()
        else:
            result = conn.execute(
                "SELECT gene_symbol, evidence_category, evidence_type, "
                "source_tag, score, tissue, cell_type "
                "FROM gene_evidence WHERE gene_symbol = ?",
                [gene],
            )
            cols = [desc[0] for desc in result.description]
            for r in result.fetchall():
                d = {c: _clean_value(v) for c, v in zip(cols, r)}
                d["evidence_level"] = "gene"
                rows.append(d)

        return rows
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/traits")
async def list_traits(request: Request):
    """List studies/traits."""
    conn = request.app.state.conn
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT study_id, trait, trait_description, gwas_source, ancestry, "
                "sample_size, n_loci FROM studies ORDER BY trait"
            )
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()
        else:
            result = conn.execute(
                "SELECT study_id, trait, trait_description, gwas_source, ancestry, "
                "sample_size, n_loci FROM studies ORDER BY trait"
            )
            cols = [desc[0] for desc in result.description]
            rows = result.fetchall()
        return [
            {c: _clean_value(v) for c, v in zip(cols, r)}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/traits/{study_id}/loci")
async def list_loci(study_id: str, request: Request):
    """List loci for a study."""
    conn = request.app.state.conn
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT locus_id, locus_name, chromosome, start_position, end_position, "
                "lead_variant_id, lead_rsid, lead_pvalue, locus_source, n_candidate_genes "
                "FROM loci WHERE study_id = %s ORDER BY chromosome, start_position",
                (study_id,),
            )
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            cur.close()
        else:
            result = conn.execute(
                "SELECT locus_id, locus_name, chromosome, start_position, end_position, "
                "lead_variant_id, lead_rsid, lead_pvalue, locus_source, n_candidate_genes "
                "FROM loci WHERE study_id = ? ORDER BY chromosome, start_position",
                [study_id],
            )
            cols = [desc[0] for desc in result.description]
            rows = result.fetchall()
        return [
            {c: _clean_value(v) for c, v in zip(cols, r)}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/loci/{locus_id}/genes")
async def locus_genes(locus_id: str, request: Request):
    """Evidence matrix for a single locus — all genes with scores and evidence."""
    conn = request.app.state.conn
    try:
        genes = []
        # Scores
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT gene_symbol, distance_to_lead_kb, is_nearest_gene, "
                "is_within_locus, integration_score, integration_rank, is_predicted_effector "
                "FROM locus_gene_scores WHERE locus_id = %s ORDER BY integration_rank",
                (locus_id,),
            )
            score_cols = [desc[0] for desc in cur.description]
            score_rows = cur.fetchall()
            cur.close()
        else:
            result = conn.execute(
                "SELECT gene_symbol, distance_to_lead_kb, is_nearest_gene, "
                "is_within_locus, integration_score, integration_rank, is_predicted_effector "
                "FROM locus_gene_scores WHERE locus_id = ? ORDER BY integration_rank",
                [locus_id],
            )
            score_cols = [desc[0] for desc in result.description]
            score_rows = result.fetchall()

        for row in score_rows:
            gene_dict = {c: _clean_value(v) for c, v in zip(score_cols, row)}
            gene_symbol = row[0]

            # Evidence for this gene at this locus
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(
                    "SELECT evidence_category, evidence_stream, source_tag, pvalue, score "
                    "FROM locus_gene_evidence WHERE locus_id = %s AND gene_symbol = %s",
                    (locus_id, gene_symbol),
                )
                ev_cols = [desc[0] for desc in cur.description]
                ev_rows = cur.fetchall()
                cur.close()
            else:
                ev_result = conn.execute(
                    "SELECT evidence_category, evidence_stream, source_tag, pvalue, score "
                    "FROM locus_gene_evidence WHERE locus_id = ? AND gene_symbol = ?",
                    [locus_id, gene_symbol],
                )
                ev_cols = [desc[0] for desc in ev_result.description]
                ev_rows = ev_result.fetchall()

            gene_dict["evidence"] = [
                {c: _clean_value(v) for c, v in zip(ev_cols, er)}
                for er in ev_rows
            ]
            genes.append(gene_dict)

        return genes
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Helpers ---

def _clean_value(val: Any) -> Any:
    """Replace None/NaN with '-' to match old Plumber API behavior."""
    if val is None:
        return "-"
    if isinstance(val, float):
        import math
        if math.isnan(val):
            return "-"
    return val
