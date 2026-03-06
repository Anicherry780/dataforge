from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class PipelineStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class DataQualityCheck(BaseModel):
    name: str
    status: str
    records_checked: int
    records_failed: int
    pass_rate: float
    details: Optional[str] = None


class PipelineStep(BaseModel):
    step_name: str
    step_type: str
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    records_in: int = 0
    records_out: int = 0
    message: Optional[str] = None


class PipelineRun(BaseModel):
    run_id: str
    pipeline_id: str
    pipeline_name: str
    status: PipelineStatus
    started_at: str
    completed_at: Optional[str] = None
    duration_ms: Optional[int] = None
    records_ingested: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    quality_score: float = 0.0
    steps: List[PipelineStep] = []
    quality_checks: List[DataQualityCheck] = []
    error_message: Optional[str] = None
    logs: List[str] = []


class PipelineDefinition(BaseModel):
    id: str
    name: str
    description: str
    source_type: str
    source_config: Dict[str, Any] = {}
    destination: str
    schedule: str
    tags: List[str] = []
    last_run: Optional[str] = None
    last_status: Optional[PipelineStatus] = None
    total_runs: int = 0
    success_rate: float = 0.0
    avg_duration_ms: int = 0
    avg_quality_score: float = 0.0


class MetricsSummary(BaseModel):
    total_records_processed: int
    total_pipeline_runs: int
    success_rate: float
    avg_quality_score: float
    records_by_day: List[Dict[str, Any]]
    quality_by_pipeline: List[Dict[str, Any]]
    pipeline_performance: List[Dict[str, Any]]


class TriggerRunRequest(BaseModel):
    dry_run: bool = False
    override_params: Dict[str, Any] = {}


class RunLogsResponse(BaseModel):
    run_id: str
    logs: List[str]
    status: PipelineStatus
