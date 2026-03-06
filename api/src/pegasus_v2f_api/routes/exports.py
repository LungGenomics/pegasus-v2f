"""Export endpoints — evidence matrix, PEG list, metadata."""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from pegasus_v2f_api.db_helpers import has_table

router = APIRouter()


@router.get("/export/{study_id}/evidence-matrix")
async def export_evidence_matrix(
    study_id: str,
    request: Request,
    format: str = Query("json", pattern="^(json|tsv)$"),
):
    """Export evidence matrix. JSON by default, TSV with ?format=tsv."""
    from pegasus_v2f.pegasus_export import export_evidence_matrix as _export

    conn = request.app.state.conn
    if not has_table(conn, "loci"):
        raise HTTPException(status_code=404, detail="No loci table — is this a PEGASUS database?")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _export(conn, study_id, Path(tmpdir))

            if format == "tsv":
                content = path.read_text()
                return StreamingResponse(
                    io.StringIO(content),
                    media_type="text/tab-separated-values",
                    headers={
                        "Content-Disposition": f'attachment; filename="evidence_matrix_{study_id}.tsv"'
                    },
                )

            # JSON: read TSV back as records
            import pandas as pd

            df = pd.read_csv(path, sep="\t")
            return df.fillna("").to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/export/{study_id}/peg-list")
async def export_peg_list(
    study_id: str,
    request: Request,
    format: str = Query("json", pattern="^(json|tsv)$"),
):
    """Export PEG list (rank-1 effector genes). JSON by default, TSV with ?format=tsv."""
    from pegasus_v2f.pegasus_export import export_peg_list as _export

    conn = request.app.state.conn
    if not has_table(conn, "scored_evidence"):
        raise HTTPException(status_code=404, detail="No scores table — is this a PEGASUS database?")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _export(conn, study_id, Path(tmpdir))

            if format == "tsv":
                content = path.read_text()
                return StreamingResponse(
                    io.StringIO(content),
                    media_type="text/tab-separated-values",
                    headers={
                        "Content-Disposition": f'attachment; filename="peg_list_{study_id}.tsv"'
                    },
                )

            import pandas as pd

            df = pd.read_csv(path, sep="\t")
            return df.fillna("").to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/export/{study_id}/metadata")
async def export_metadata(
    study_id: str,
    request: Request,
    format: str = Query("json", pattern="^(json|yaml)$"),
):
    """Export PEGASUS metadata. JSON by default, YAML with ?format=yaml."""
    from pegasus_v2f.pegasus_export import export_metadata as _export

    conn = request.app.state.conn
    if not has_table(conn, "studies"):
        raise HTTPException(status_code=404, detail="No studies table — is this a PEGASUS database?")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _export(conn, study_id, Path(tmpdir))
            content = path.read_text()

            if format == "yaml":
                return StreamingResponse(
                    io.StringIO(content),
                    media_type="text/yaml",
                    headers={
                        "Content-Disposition": f'attachment; filename="metadata_{study_id}.yaml"'
                    },
                )

            # JSON: parse the YAML and return as JSON-serializable dict
            return yaml.safe_load(content) or {}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
