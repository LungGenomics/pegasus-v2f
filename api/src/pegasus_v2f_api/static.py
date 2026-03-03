"""Serve React UI with SPA routing."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles


def mount_static(app: FastAPI) -> None:
    """Mount the React UI static files with SPA fallback routing.

    Looks for ui/dist/ relative to the monorepo root.
    If no built UI exists, serves a placeholder page.
    """
    # Find the ui/dist directory
    dist_dir = _find_ui_dist()

    if dist_dir and dist_dir.exists():
        # Serve static assets (js, css, images)
        app.mount("/assets", StaticFiles(directory=dist_dir / "assets"), name="assets")

        @app.get("/ui/{path:path}")
        async def spa_route(path: str):
            """SPA fallback — serve index.html for all /ui/* routes."""
            # Check if it's a real file
            file_path = dist_dir / path
            if file_path.is_file():
                return FileResponse(file_path)
            return FileResponse(dist_dir / "index.html")

        @app.get("/ui")
        async def ui_root():
            return FileResponse(dist_dir / "index.html")
    else:
        @app.get("/ui")
        @app.get("/ui/{path:path}")
        async def ui_placeholder(path: str = ""):
            return HTMLResponse(
                "<html><body><h1>PEGASUS V2F</h1>"
                "<p>React UI not built yet. Run <code>npm run build</code> in <code>ui/</code>.</p>"
                "<p>API is available at <code>/api/</code>.</p>"
                "</body></html>"
            )

    # Redirect / to /ui
    @app.get("/")
    async def root_redirect():
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/ui")


def _find_ui_dist() -> Path | None:
    """Find the ui/dist directory by walking up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        candidate = current / "ui" / "dist"
        if candidate.exists():
            return candidate
        current = current.parent
    return None
