"""
Integration tests for the FastAPI application.
Tests all REST endpoints and the pipeline trigger flow.
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.database import init_db


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio(loop_scope="session")
class TestHealthEndpoint:
    async def test_health_returns_200(self, client):
        r = await client.get("/api/health")
        assert r.status_code == 200

    async def test_health_response_shape(self, client):
        r = await client.get("/api/health")
        data = r.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert "timestamp" in data


@pytest.mark.asyncio(loop_scope="session")
class TestPipelinesEndpoint:
    async def test_list_pipelines_returns_200(self, client):
        r = await client.get("/api/pipelines")
        assert r.status_code == 200

    async def test_list_pipelines_returns_all_registered(self, client):
        r = await client.get("/api/pipelines")
        data = r.json()
        assert "pipelines" in data
        assert len(data["pipelines"]) == 3  # crypto-market, weather-observations, github-events

    async def test_pipeline_has_required_fields(self, client):
        r = await client.get("/api/pipelines")
        pipelines = r.json()["pipelines"]
        required = {"id", "name", "description", "source_type", "destination", "schedule"}
        for p in pipelines:
            assert required.issubset(set(p.keys()))

    async def test_get_single_pipeline(self, client):
        r = await client.get("/api/pipelines/crypto-market")
        assert r.status_code == 200
        data = r.json()
        assert data["pipeline"]["id"] == "crypto-market"

    async def test_get_unknown_pipeline_returns_404(self, client):
        r = await client.get("/api/pipelines/does-not-exist")
        assert r.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
class TestTriggerEndpoint:
    async def test_trigger_returns_run_id(self, client):
        r = await client.post(
            "/api/pipelines/weather-observations/trigger",
            json={"dry_run": True},
        )
        assert r.status_code == 200
        data = r.json()
        assert "run_id" in data
        assert data["run_id"].startswith("run-")
        assert data["status"] == "running"

    async def test_trigger_unknown_pipeline_returns_404(self, client):
        r = await client.post("/api/pipelines/ghost-pipeline/trigger", json={})
        assert r.status_code == 404

    async def test_dry_run_flag_reflected(self, client):
        r = await client.post(
            "/api/pipelines/github-events/trigger",
            json={"dry_run": True},
        )
        assert r.status_code == 200
        assert r.json()["dry_run"] is True


@pytest.mark.asyncio(loop_scope="session")
class TestRunsEndpoint:
    async def test_list_runs_returns_200(self, client):
        r = await client.get("/api/runs")
        assert r.status_code == 200
        data = r.json()
        assert "runs" in data
        assert isinstance(data["runs"], list)

    async def test_get_run_not_found(self, client):
        r = await client.get("/api/runs/run-doesnotexist")
        assert r.status_code == 404

    async def test_get_run_logs_not_found(self, client):
        r = await client.get("/api/runs/run-ghost/logs")
        assert r.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
class TestDataEndpoint:
    async def test_data_preview_returns_200(self, client):
        r = await client.get("/api/data/preview")
        assert r.status_code == 200
        data = r.json()
        assert "records" in data
        assert "count" in data

    async def test_data_preview_limit_param(self, client):
        r = await client.get("/api/data/preview?limit=5")
        assert r.status_code == 200
        data = r.json()
        assert len(data["records"]) <= 5


@pytest.mark.asyncio(loop_scope="session")
class TestMetricsEndpoint:
    async def test_metrics_returns_200(self, client):
        r = await client.get("/api/metrics")
        assert r.status_code == 200

    async def test_metrics_has_required_keys(self, client):
        r = await client.get("/api/metrics")
        data = r.json()
        required = {
            "total_records_processed", "total_pipeline_runs",
            "success_rate", "avg_quality_score",
            "records_by_day", "quality_by_pipeline", "pipeline_performance",
        }
        assert required.issubset(set(data.keys()))

