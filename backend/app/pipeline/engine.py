"""
Pipeline Execution Engine
Orchestrates ingestion → quality checks → transformation → load.
Emits real-time progress events via the WebSocket connection manager.
"""
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from app.core.connection_manager import manager
from app.database import save_run
from app.pipeline.ingestion import get_ingestion_source
from app.pipeline.quality import run_all_checks
from app.pipeline.transformation import transform
from app.pipeline.loader import load_to_warehouse


PIPELINE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "crypto-market": {
        "id": "crypto-market",
        "name": "Crypto Market Data",
        "description": (
            "Fetches live cryptocurrency market data from CoinGecko — "
            "prices, volumes, market caps, and 24h changes for the top 100 coins."
        ),
        "source_type": "crypto",
        "destination": "SQLite / crypto_records",
        "schedule": "*/5 * * * *",
        "tags": ["crypto", "coingecko", "market-data", "real-time"],
        "source_config": {"top_n": 100},
    },
    "weather-observations": {
        "id": "weather-observations",
        "name": "Global Weather Observations",
        "description": (
            "Collects current weather conditions for 10 global cities via Open-Meteo API — "
            "temperature, humidity, wind speed, precipitation, and weather code."
        ),
        "source_type": "weather",
        "destination": "SQLite / weather_records",
        "schedule": "0 * * * *",
        "tags": ["weather", "open-meteo", "climate", "global"],
        "source_config": {},
    },
    "github-events": {
        "id": "github-events",
        "name": "GitHub Public Events",
        "description": (
            "Streams real developer activity from GitHub's public events API — "
            "pushes, pull requests, issues, forks, and more."
        ),
        "source_type": "github",
        "destination": "SQLite / github_records",
        "schedule": "*/15 * * * *",
        "tags": ["github", "developer", "events", "streaming"],
        "source_config": {"max_pages": 5},
    },
}


async def _emit(run_id: str, event: str, payload: Dict[str, Any]):
    """Broadcast a structured event to all WebSocket listeners for this run."""
    await manager.broadcast(run_id, {"event": event, "run_id": run_id, **payload})


async def execute_pipeline(
    pipeline_id: str,
    dry_run: bool = False,
) -> str:
    """
    Execute a pipeline by ID. Returns the run_id.
    Actual execution happens in the background — progress is streamed via WebSocket.
    """
    if pipeline_id not in PIPELINE_REGISTRY:
        raise ValueError(f"Pipeline '{pipeline_id}' not found.")

    run_id = f"run-{uuid.uuid4().hex[:12]}"
    asyncio.create_task(_run_pipeline(pipeline_id, run_id, dry_run))
    return run_id


async def _run_pipeline(pipeline_id: str, run_id: str, dry_run: bool):
    defn = PIPELINE_REGISTRY[pipeline_id]
    source_type = defn["source_type"]
    started_at = datetime.now(timezone.utc).isoformat()
    logs = []
    steps = []

    def log(msg: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        logs.append(entry)
        return entry

    async def persist(status: str, extra: Optional[Dict] = None):
        payload = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": defn["name"],
            "status": status,
            "started_at": started_at,
            "steps": steps,
            "logs": logs,
            **(extra or {}),
        }
        await save_run(payload)

    async def progress_cb(done: int, total: int, label: str):
        pct = round(done / total * 100) if total else 0
        await _emit(run_id, "progress", {"label": label, "done": done, "total": total, "pct": pct})

    # ── Initial state ─────────────────────────────────────────────────────────
    await persist("running")
    log(f"Pipeline '{defn['name']}' started (run_id={run_id}, dry_run={dry_run})")
    await _emit(run_id, "started", {"pipeline_name": defn["name"], "dry_run": dry_run})

    try:
        # ── STEP 1: Ingestion ──────────────────────────────────────────────────
        log(f"[INGEST] Connecting to source: {source_type.upper()}")
        await _emit(run_id, "step_start", {"step": "ingestion"})

        source = get_ingestion_source(source_type, defn.get("source_config", {}))
        raw_records = await source.fetch(on_progress=progress_cb)
        records_ingested = len(raw_records)

        log(f"[INGEST] Fetched {records_ingested:,} records from {source_type}")
        steps.append({"step_name": "Ingestion", "step_type": "ingestion",
                       "status": "success", "records_out": records_ingested})
        await _emit(run_id, "step_done", {"step": "ingestion", "records": records_ingested})
        await persist("running", {"records_ingested": records_ingested})

        # ── STEP 2: Data Quality ───────────────────────────────────────────────
        log("[DQ] Running data quality checks...")
        await _emit(run_id, "step_start", {"step": "quality"})

        quality_checks, quality_score = run_all_checks(raw_records, source_type=source_type)
        passed = sum(1 for c in quality_checks if c["status"] == "pass")
        log(f"[DQ] Quality score: {quality_score:.1f}% ({passed}/{len(quality_checks)} checks passed)")

        for chk in quality_checks:
            status_icon = "✓" if chk["status"] == "pass" else ("⚠" if chk["status"] == "warn" else "✗")
            log(f"[DQ]   {status_icon} {chk['name']}: {chk['pass_rate']:.1f}% — {chk['details']}")

        steps.append({"step_name": "Data Quality", "step_type": "quality",
                       "status": "success", "records_in": records_ingested,
                       "records_out": records_ingested, "message": f"Score: {quality_score:.1f}%"})
        await _emit(run_id, "step_done", {
            "step": "quality", "quality_score": quality_score,
            "quality_checks": quality_checks,
        })
        await persist("running", {
            "records_ingested": records_ingested,
            "quality_score": quality_score,
            "quality_checks": quality_checks,
            "logs": logs,
        })

        # ── STEP 3: Transformation ─────────────────────────────────────────────
        log("[TRANSFORM] Starting transformations...")
        await _emit(run_id, "step_start", {"step": "transformation"})

        transformed, aggregations = await transform(
            raw_records, source_type=source_type, on_progress=progress_cb
        )
        records_transformed = len(transformed)

        log(f"[TRANSFORM] {records_transformed:,} records transformed")
        log(f"[TRANSFORM] Anomalies flagged: {aggregations.get('anomalies_detected', 0)}")
        # Log a few key aggregation metrics
        for k, v in aggregations.items():
            if k != "anomalies_detected" and not isinstance(v, dict):
                log(f"[TRANSFORM] {k}: {v}")

        steps.append({"step_name": "Transformation", "step_type": "transformation",
                       "status": "success", "records_in": records_ingested,
                       "records_out": records_transformed})
        await _emit(run_id, "step_done", {
            "step": "transformation",
            "records": records_transformed,
            "aggregations": aggregations,
            "source_type": source_type,
        })
        await persist("running", {
            "records_ingested": records_ingested,
            "records_transformed": records_transformed,
            "quality_score": quality_score,
            "quality_checks": quality_checks,
            "steps": steps,
            "logs": logs,
        })

        # ── STEP 4: Load ───────────────────────────────────────────────────────
        if dry_run:
            log("[LOAD] DRY RUN — skipping load step")
            records_loaded = 0
            steps.append({"step_name": "Load", "step_type": "load",
                           "status": "skipped", "message": "dry_run=True"})
        else:
            log(f"[LOAD] Loading {records_transformed:,} records to warehouse...")
            await _emit(run_id, "step_start", {"step": "load"})

            load_result = await load_to_warehouse(
                transformed, run_id, pipeline_id=pipeline_id, on_progress=progress_cb
            )
            records_loaded = load_result["records_loaded"]
            log(f"[LOAD] {records_loaded:,} records loaded in {load_result['batches']} batches")

            steps.append({"step_name": "Load", "step_type": "load",
                           "status": "success", "records_in": records_transformed,
                           "records_out": records_loaded})
            await _emit(run_id, "step_done", {"step": "load", "records": records_loaded})

        # ── Done ───────────────────────────────────────────────────────────────
        completed_at = datetime.now(timezone.utc).isoformat()
        started_dt = datetime.fromisoformat(started_at)
        completed_dt = datetime.fromisoformat(completed_at)
        duration_ms = int((completed_dt - started_dt).total_seconds() * 1000)

        log(f"[DONE] Pipeline completed in {duration_ms / 1000:.1f}s")
        log(f"[DONE] Summary: ingested={records_ingested:,} | "
            f"transformed={records_transformed:,} | loaded={records_loaded:,} | "
            f"quality={quality_score:.1f}%")

        final = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": defn["name"],
            "status": "success",
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": duration_ms,
            "records_ingested": records_ingested,
            "records_transformed": records_transformed,
            "records_loaded": records_loaded,
            "quality_score": quality_score,
            "quality_checks": quality_checks,
            "steps": steps,
            "logs": logs,
        }
        await save_run(final)
        await _emit(run_id, "completed", {
            "status": "success",
            "duration_ms": duration_ms,
            "records_loaded": records_loaded,
            "quality_score": quality_score,
            "aggregations": aggregations,
            "source_type": source_type,
        })

    except Exception as exc:
        error_msg = str(exc)
        log(f"[ERROR] {error_msg}")
        completed_at = datetime.now(timezone.utc).isoformat()
        await save_run({
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": defn["name"],
            "status": "failed",
            "started_at": started_at,
            "completed_at": completed_at,
            "steps": steps,
            "logs": logs,
            "error_message": error_msg,
        })
        await _emit(run_id, "failed", {"error": error_msg})
