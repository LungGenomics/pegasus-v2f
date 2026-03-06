"""Studies, loci, and locus-gene evidence matrix endpoints."""

from __future__ import annotations

from collections import OrderedDict

from fastapi import APIRouter, HTTPException, Request

from pegasus_v2f_api.db_helpers import clean_rows, clean_value, execute_query, has_table

router = APIRouter()


# --- Studies ---

@router.get("/studies")
async def list_studies(request: Request):
    """List studies/traits."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "studies"):
            return []

        columns, rows = execute_query(
            conn,
            "SELECT study_id, trait, trait_description, trait_ontology_id, "
            "study_description, gwas_source, ancestry, sample_size, doi, year, n_loci "
            "FROM studies ORDER BY trait",
        )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- All loci (cross-study) ---

@router.get("/loci")
async def list_all_loci(request: Request, limit: int = 500):
    """List all loci across all studies."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "loci"):
            return []

        has_scores = has_table(conn, "scored_evidence")
        if has_scores:
            columns, rows = execute_query(
                conn,
                "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, "
                "l.end_position, l.lead_rsid, l.lead_pvalue, l.n_candidate_genes, "
                "l.study_id, s.trait, "
                "sc.gene_symbol AS top_gene "
                "FROM loci l "
                "LEFT JOIN studies s ON l.study_id = s.study_id "
                "LEFT JOIN (SELECT DISTINCT locus_id, gene_symbol, integration_rank "
                "FROM scored_evidence WHERE integration_rank = 1) sc ON l.locus_id = sc.locus_id "
                "ORDER BY l.chromosome, l.start_position "
                "LIMIT ?",
                [limit],
            )
        else:
            columns, rows = execute_query(
                conn,
                "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, "
                "l.end_position, l.lead_rsid, l.lead_pvalue, l.n_candidate_genes, "
                "l.study_id, s.trait "
                "FROM loci l LEFT JOIN studies s ON l.study_id = s.study_id "
                "ORDER BY l.chromosome, l.start_position "
                "LIMIT ?",
                [limit],
            )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Study detail ---

@router.get("/studies/{study_id}")
async def get_study(study_id: str, request: Request):
    """Get a single study with summary statistics."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "studies"):
            raise HTTPException(status_code=404, detail=f"Study '{study_id}' not found")

        columns, rows = execute_query(
            conn, "SELECT * FROM studies WHERE study_id = ?", [study_id],
        )
        if not rows:
            raise HTTPException(status_code=404, detail=f"Study '{study_id}' not found")
        study = clean_rows(columns, rows)[0]

        # Aggregate stats
        if has_table(conn, "loci"):
            _, r = execute_query(
                conn, "SELECT COUNT(*) FROM loci WHERE study_id = ?", [study_id],
            )
            study["n_loci_actual"] = r[0][0] if r else 0

        if has_table(conn, "scored_evidence"):
            _, r = execute_query(
                conn,
                "SELECT COUNT(DISTINCT s.gene_symbol) FROM scored_evidence s "
                "WHERE s.study_id = ?",
                [study_id],
            )
            study["n_candidate_genes"] = r[0][0] if r else 0

            _, r = execute_query(
                conn,
                "SELECT COUNT(DISTINCT s.gene_symbol) FROM scored_evidence s "
                "WHERE s.study_id = ? AND s.is_predicted_effector = TRUE",
                [study_id],
            )
            study["n_effectors"] = r[0][0] if r else 0

            _, r = execute_query(
                conn,
                "SELECT DISTINCT evidence_category FROM scored_evidence "
                "WHERE study_id = ? AND evidence_category IS NOT NULL",
                [study_id],
            )
            study["evidence_categories"] = sorted(row[0] for row in r)

        return study
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Study loci ---

@router.get("/studies/{study_id}/loci")
async def list_loci(study_id: str, request: Request):
    """List loci for a study."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "loci"):
            return []

        has_scores = has_table(conn, "scored_evidence")
        if has_scores:
            columns, rows = execute_query(
                conn,
                "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
                "l.lead_variant_id, l.lead_rsid, l.lead_pvalue, l.locus_source, "
                "l.n_signals, l.n_candidate_genes, "
                "s.gene_symbol AS top_gene "
                "FROM loci l "
                "LEFT JOIN (SELECT DISTINCT locus_id, gene_symbol, integration_rank "
                "FROM scored_evidence WHERE integration_rank = 1) s ON l.locus_id = s.locus_id "
                "WHERE l.study_id = ? ORDER BY l.chromosome, l.start_position",
                [study_id],
            )
        else:
            columns, rows = execute_query(
                conn,
                "SELECT locus_id, locus_name, chromosome, start_position, end_position, "
                "lead_variant_id, lead_rsid, lead_pvalue, locus_source, n_signals, n_candidate_genes "
                "FROM loci WHERE study_id = ? ORDER BY chromosome, start_position",
                [study_id],
            )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Study effectors ---

@router.get("/studies/{study_id}/effectors")
async def list_effectors(study_id: str, request: Request):
    """PEG list — rank-1 predicted effector genes for a study."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "scored_evidence"):
            return []

        # One row per locus — the rank-1 gene
        columns, rows = execute_query(
            conn,
            "SELECT l.locus_id, l.locus_name, l.chromosome, l.start_position, l.end_position, "
            "s.gene_symbol, s.integration_rank, s.is_predicted_effector "
            "FROM (SELECT DISTINCT locus_id, study_id, gene_symbol, integration_rank, is_predicted_effector "
            "FROM scored_evidence WHERE integration_rank = 1) s "
            "JOIN loci l ON s.locus_id = l.locus_id "
            "WHERE s.study_id = ? "
            "ORDER BY l.chromosome, l.start_position",
            [study_id],
        )
        return clean_rows(columns, rows)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Locus genes (evidence matrix) ---

@router.get("/loci/{locus_id}/genes")
async def locus_genes(locus_id: str, request: Request):
    """Evidence matrix for a single locus — all genes with scores and evidence."""
    conn = request.app.state.conn
    try:
        if not has_table(conn, "scored_evidence"):
            return []

        columns, rows = execute_query(
            conn,
            "SELECT gene_symbol, evidence_category, source_tag, trait, "
            "pvalue, effect_size, score, tissue, cell_type, rsid, "
            "match_type, integration_rank, is_predicted_effector, n_candidate_genes "
            "FROM scored_evidence "
            "WHERE locus_id = ? "
            "ORDER BY integration_rank, evidence_category",
            [locus_id],
        )

        # Group evidence under each gene
        col_idx = {c: i for i, c in enumerate(columns)}
        gene_fields = ["gene_symbol", "integration_rank", "is_predicted_effector", "n_candidate_genes"]
        evidence_fields = [
            "evidence_category", "source_tag", "trait", "pvalue",
            "effect_size", "score", "tissue", "cell_type", "rsid", "match_type",
        ]

        genes: OrderedDict[str, dict] = OrderedDict()

        for row in rows:
            gene_symbol = row[col_idx["gene_symbol"]]
            if gene_symbol not in genes:
                genes[gene_symbol] = {
                    f: clean_value(row[col_idx[f]]) for f in gene_fields
                }
                genes[gene_symbol]["evidence"] = []

            ev_category = row[col_idx["evidence_category"]]
            if ev_category is not None:
                ev = {f: clean_value(row[col_idx[f]]) for f in evidence_fields}
                genes[gene_symbol]["evidence"].append(ev)

        return list(genes.values())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Study preview ---

@router.get("/studies/{study_name}/preview")
async def study_preview(study_name: str, request: Request):
    """Preview scoring — candidate genes and evidence per locus without materializing."""
    conn = request.app.state.conn
    try:
        from pegasus_v2f.study_management import preview_study
        results = preview_study(conn, study_name)
        return results
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --- Catch-all for study IDs containing slashes (e.g. FEV1/FVC) ---
# The {study_id} routes above only match a single path segment. When the ID
# contains a "/" it gets decoded by the HTTP stack, producing extra segments.
# This catch-all uses :path to capture everything and dispatches by suffix.

@router.get("/studies/{rest:path}")
async def study_path_fallback(rest: str, request: Request):
    """Route study requests where the study_id contains slashes."""
    if rest.endswith("/loci"):
        return await list_loci(rest.removesuffix("/loci"), request)
    if rest.endswith("/effectors"):
        return await list_effectors(rest.removesuffix("/effectors"), request)
    if rest.endswith("/preview"):
        return await study_preview(rest.removesuffix("/preview"), request)
    return await get_study(rest, request)


# --- Backward compatibility aliases ---

@router.get("/traits")
async def list_traits(request: Request):
    """Alias for /studies (backward compat)."""
    return await list_studies(request)


@router.get("/traits/{study_id}/loci")
async def list_loci_compat(study_id: str, request: Request):
    """Alias for /studies/{study_id}/loci (backward compat)."""
    return await list_loci(study_id, request)
