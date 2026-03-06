from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, timezone

from app.models import TriggerRunRequest
from app.database import (
    get_run, get_all_runs, get_runs_by_pipeline,
    get_data_preview, get_metrics_summary,
)
from app.pipeline.engine import PIPELINE_REGISTRY, execute_pipeline

router = APIRouter()


# ── Pipelines ──────────────────────────────────────────────────────────────────

@router.get("/pipelines", summary="List all pipeline definitions")
async def list_pipelines():
    runs = await get_all_runs(limit=500)

    # Annotate each definition with live stats from DB
    enriched = []
    for pid, defn in PIPELINE_REGISTRY.items():
        pipeline_runs = [r for r in runs if r["pipeline_id"] == pid]
        total = len(pipeline_runs)
        successes = sum(1 for r in pipeline_runs if r["status"] == "success")
        durations = [r["duration_ms"] for r in pipeline_runs if r.get("duration_ms")]
        last_run = pipeline_runs[0] if pipeline_runs else None

        enriched.append({
            **defn,
            "total_runs": total,
            "success_rate": round(successes / total * 100, 1) if total else 0.0,
            "avg_duration_ms": int(sum(durations) / len(durations)) if durations else 0,
            "avg_quality_score": round(
                sum(r.get("quality_score", 0) for r in pipeline_runs) / total, 1
            ) if total else 0.0,
            "last_run": last_run["started_at"] if last_run else None,
            "last_status": last_run["status"] if last_run else None,
        })

    return {"pipelines": enriched}


@router.get("/pipelines/{pipeline_id}", summary="Get a single pipeline definition")
async def get_pipeline(pipeline_id: str):
    if pipeline_id not in PIPELINE_REGISTRY:
        raise HTTPException(404, f"Pipeline '{pipeline_id}' not found")
    defn = PIPELINE_REGISTRY[pipeline_id]
    pipeline_runs = await get_runs_by_pipeline(pipeline_id, limit=10)
    return {"pipeline": defn, "recent_runs": pipeline_runs}


# ── Runs ───────────────────────────────────────────────────────────────────────

@router.post("/pipelines/{pipeline_id}/trigger", summary="Trigger a pipeline run")
async def trigger_pipeline(pipeline_id: str, body: TriggerRunRequest = TriggerRunRequest()):
    if pipeline_id not in PIPELINE_REGISTRY:
        raise HTTPException(404, f"Pipeline '{pipeline_id}' not found")

    run_id = await execute_pipeline(pipeline_id, dry_run=body.dry_run)
    return {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "status": "running",
        "message": "Pipeline started. Connect to /ws/runs/{run_id} for real-time updates.",
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": body.dry_run,
    }


@router.get("/runs", summary="List recent pipeline runs")
async def list_runs(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline"),
    limit: int = Query(20, ge=1, le=100),
):
    if pipeline_id:
        runs = await get_runs_by_pipeline(pipeline_id, limit=limit)
    else:
        runs = await get_all_runs(limit=limit)
    return {"runs": runs, "total": len(runs)}


@router.get("/runs/{run_id}", summary="Get a specific run's status and details")
async def get_run_status(run_id: str):
    run = await get_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    return run


@router.get("/runs/{run_id}/logs", summary="Get logs for a specific run")
async def get_run_logs(run_id: str):
    run = await get_run(run_id)
    if not run:
        raise HTTPException(404, f"Run '{run_id}' not found")
    return {
        "run_id": run_id,
        "status": run["status"],
        "logs": run.get("logs", []),
    }


# ── Data Preview ───────────────────────────────────────────────────────────────

@router.get("/data/preview", summary="Preview the latest processed records")
async def preview_data(
    run_id: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None, description="Filter by source: crypto, weather, github"),
    limit: int = Query(50, ge=1, le=500),
):
    records = await get_data_preview(run_id=run_id, source_type=source_type, limit=limit)
    return {
        "records": records,
        "count": len(records),
        "run_id": run_id,
        "source_type": source_type,
    }


# ── Metrics ────────────────────────────────────────────────────────────────────

@router.get("/metrics", summary="Get aggregated platform metrics")
async def get_metrics():
    metrics = await get_metrics_summary()
    return metrics


# ── Health ─────────────────────────────────────────────────────────────────────

@router.get("/health", summary="Health check endpoint")
async def health():
    return {
        "status": "healthy",
        "service": "DataForge API",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
