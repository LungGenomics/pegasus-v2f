"""FastAPI app factory for PEGASUS V2F."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def create_app(
    db: str | None = None,
    config: dict | None = None,
    project_root: str | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        db: Database path or connection string.
        config: Resolved config dict (optional).
        project_root: Project root for resolving relative DB paths.
    """
    state: dict[str, Any] = {"db": db, "config": config, "conn": None, "project_root": project_root}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from pegasus_v2f.db import get_connection

        db_arg = state["db"] or os.environ.get("V2F_DATABASE_URL")
        state["conn"] = get_connection(
            db=db_arg, config=state["config"], read_only=False,
            project_root=state["project_root"],
        )
        app.state.conn = state["conn"]
        app.state.config = state["config"]
        yield
        if state["conn"]:
            state["conn"].close()

    app = FastAPI(
        title="PEGASUS V2F",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from pegasus_v2f_api.routes.genes import router as genes_router
    from pegasus_v2f_api.routes.studies import router as studies_router
    from pegasus_v2f_api.routes.sources import router as sources_router
    from pegasus_v2f_api.routes.exports import router as exports_router
    from pegasus_v2f_api.routes.db import router as db_router

    app.include_router(genes_router, prefix="/api")
    app.include_router(studies_router, prefix="/api")
    app.include_router(sources_router, prefix="/api")
    app.include_router(exports_router, prefix="/api")
    app.include_router(db_router, prefix="/api")

    from pegasus_v2f_api.static import mount_static
    mount_static(app)

    return app
