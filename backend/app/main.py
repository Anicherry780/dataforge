from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
import pathlib

from app.database import init_db
from app.api.routes import router
from app.core.connection_manager import manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="DataForge API",
    description=(
        "Real-Time ETL Pipeline & Analytics Platform. "
        "Build, monitor, and analyze data pipelines with live quality metrics."
    ),
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

_allowed_origins = os.environ.get(
    "ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:80"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials="*" not in _allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


# ── WebSocket: real-time pipeline run updates ─────────────────────────────────

@app.websocket("/ws/runs/{run_id}")
async def websocket_run(websocket: WebSocket, run_id: str):
    """
    Connect to receive real-time progress events for a pipeline run.
    Events: started | step_start | step_done | progress | completed | failed
    """
    await manager.connect(websocket, run_id)
    try:
        # Keep the connection alive — pipeline engine pushes events to the client
        while True:
            # Client can send 'ping' to test liveness
            data = await websocket.receive_text()
            if data.strip() == "ping":
                await websocket.send_text('{"event":"pong"}')
    except WebSocketDisconnect:
        await manager.disconnect(websocket, run_id)


# ── Serve frontend ─────────────────────────────────────────────────────────────

FRONTEND_DIR = pathlib.Path(__file__).parent.parent.parent / "frontend"

if FRONTEND_DIR.exists():
    @app.get("/", include_in_schema=False)
    async def serve_frontend():
        index = FRONTEND_DIR / "index.html"
        return FileResponse(str(index))

    @app.get("/styles.css", include_in_schema=False)
    async def serve_css():
        return FileResponse(str(FRONTEND_DIR / "styles.css"), media_type="text/css")

    @app.get("/app.js", include_in_schema=False)
    async def serve_js():
        return FileResponse(str(FRONTEND_DIR / "app.js"), media_type="application/javascript")
