"""Gene search, detail, evidence, and scores endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from pegasus_v2f.db import is_postgres
from pegasus_v2f_api.db_helpers import clean_rows, clean_value, execute_query, has_table

router = APIRouter()


def _gene_col(conn) -> str:
    """Return the gene name column in gene_search_index ('gene' or 'gene_symbol')."""
    try:
        cols, _ = execute_query(conn, "SELECT * FROM gene_search_index LIMIT 0", [])
        if "gene_symbol" in cols:
            return "gene_symbol"
    except Exception:
        pass
    return "gene"


# --- Gene search ---

@router.get("/genes")
async def search_genes(
    request: Request, search: str = "", limit: int = 50, offset: int = 0,
):
    """Search genes by name or symbol. Returns {results, total}."""
    conn = request.app.state.conn
    gcol = _gene_col(conn)

    if not search:
        try:
            columns, rows = execute_query(
                conn,
                f'SELECT *, {gcol} AS gene FROM gene_search_index ORDER BY {gcol} LIMIT ? OFFSET ?',
                [limit, offset],
            )
            _, count_rows = execute_query(
                conn, 'SELECT COUNT(*) FROM gene_search_index', [],
            )
            return {"results": clean_rows(columns, rows), "total": count_rows[0][0]}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    try:
        if is_postgres(conn):
            columns, rows = execute_query(
                conn,
                f"SELECT *, {gcol} AS gene FROM gene_search_index WHERE {gcol} ILIKE ? ORDER BY {gcol} LIMIT ? OFFSET ?",
                [f"%{search}%", limit, offset],
            )
            _, count_rows = execute_query(
                conn,
                f"SELECT COUNT(*) FROM gene_search_index WHERE {gcol} ILIKE ?",
                [f"%{search}%"],
            )
        else:
            # Try FTS first, fall back to LIKE
            try:
                fts_key = "gene_symbol" if gcol == "gene_symbol" else "ensembl_gene_id"
                columns, rows = execute_query(
                    conn,
                    f"""SELECT gsi.*, gsi.{gcol} AS gene, fts.score
                    FROM fts_main_gene_search_index fts
                    JOIN gene_search_index gsi ON gsi.{fts_key} = fts.{fts_key}
                    WHERE fts.match_bm25({fts_key}, ?) IS NOT NULL
                    ORDER BY fts.score DESC LIMIT ? OFFSET ?""",
                    [search, limit, offset],
                )
                _, count_rows = execute_query(
                    conn,
                    f"""SELECT COUNT(*)
                    FROM fts_main_gene_search_index fts
                    JOIN gene_search_index gsi ON gsi.{fts_key} = fts.{fts_key}
                    WHERE fts.match_bm25({fts_key}, ?) IS NOT NULL""",
                    [search],
                )
            except Exception:
                columns, rows = execute_query(
                    conn,
                    f"SELECT *, {gcol} AS gene FROM gene_search_index WHERE {gcol} LIKE ? OR searchable_text LIKE ? ORDER BY {gcol} LIMIT ? OFFSET ?",
                    [f"%{search}%", f"%{search}%", limit, offset],
                )
                _, count_rows = execute_query(
                    conn,
                    f"SELECT COUNT(*) FROM gene_search_index WHERE {gcol} LIKE ? OR searchable_text LIKE ?",
                    [f"%{search}%", f"%{search}%"],
                )

        return {"results": clean_rows(columns, rows), "total": count_rows[0][0]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Gene detail ---

@router.get("/genes/{gene}")
async def get_gene(gene: str, request: Request):
    """Get detail for a single gene."""
    conn = request.app.state.conn
    try:
        rows = []
        if has_table(conn, "genes"):
            columns, rows = execute_query(
                conn,
                "SELECT * FROM genes WHERE gene_symbol = ?",
                [gene],
            )
        if not rows and has_table(conn, "gene_search_index"):
            # Fallback: gene may exist in evidence but not have Ensembl annotations
            gcol = _gene_col(conn)
            columns, rows = execute_query(
                conn,
                f"SELECT * FROM gene_search_index WHERE {gcol} = ?",
                [gene],
            )

        if not rows:
            raise HTTPException(status_code=404, detail=f"Gene '{gene}' not found")
        return clean_rows(columns, rows)[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Gene evidence ---

@router.get("/genes/{gene}/evidence")
async def get_gene_evidence(gene: str, request: Request):
    """Get all evidence for a gene (locus-level + gene-level)."""
    conn = request.app.state.conn
    try:
        results = []

        # All evidence for this gene (unified table)
        if has_table(conn, "evidence"):
            columns, rows = execute_query(
                conn,
                "SELECT gene_symbol, chromosome, position, rsid, evidence_category, "
                "source_tag, trait, pvalue, effect_size, score, tissue, cell_type, "
                "ancestry, sex, evidence_stream, is_supporting "
                "FROM evidence WHERE gene_symbol = ?",
                [gene],
            )
            for row in rows:
                d = {c: clean_value(v) for c, v in zip(columns, row)}
                # Variant-level evidence has chr/pos; gene-level has nulls
                d["evidence_level"] = "variant" if d.get("chromosome") else "gene"
                results.append(d)

        return results
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Gene scores ---

@router.get("/genes/{gene}/scores")
async def get_gene_scores(gene: str, request: Request):
    """Get all locus scores for a gene (which loci, rank, is_effector)."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "scored_evidence"):
            return []

        # Return one row per locus-gene with rank and effector status
        columns, rows = execute_query(
            conn,
            "SELECT DISTINCT s.locus_id, s.gene_symbol, s.integration_rank, "
            "s.is_predicted_effector, s.match_type, s.n_candidate_genes, "
            "l.locus_name, l.chromosome, l.start_position, l.end_position, l.study_id "
            "FROM scored_evidence s "
            "JOIN loci l ON s.locus_id = l.locus_id "
            "WHERE s.gene_symbol = ? "
            "ORDER BY s.integration_rank",
            [gene],
        )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Backward compatibility alias ---

@router.get("/evidence/{gene}")
async def get_gene_evidence_compat(gene: str, request: Request):
    """Alias for /genes/{gene}/evidence (backward compat)."""
    return await get_gene_evidence(gene, request)
