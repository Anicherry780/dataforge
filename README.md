# ◈ DataForge — Real-Time ETL Pipeline & Analytics Platform

> A production-grade data engineering platform for ingesting, transforming, validating, and loading large-scale datasets — with a live monitoring dashboard and WebSocket-powered real-time updates.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=flat&logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?style=flat&logo=githubactions&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## Overview

DataForge is a full-stack data engineering platform that demonstrates end-to-end ETL pipeline design, real-time data quality monitoring, and scalable API architecture. Built to simulate a production environment used by e-commerce companies to process millions of daily transactions.

**Key capabilities:**
- Multi-source data ingestion (CSV files, REST APIs, streaming/Kafka simulation)
- Statistical anomaly detection using Z-score analysis
- 5-dimension data quality framework (Completeness, Validity, Uniqueness, Timeliness, Consistency)
- Real-time pipeline progress via WebSockets
- Interactive analytics dashboard with live charts

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DataForge Platform                           │
│                                                                     │
│  ┌──────────────┐    ┌────────────────────────────────────────────┐ │
│  │   Frontend   │    │              Pipeline Engine               │ │
│  │              │    │                                            │ │
│  │  Dashboard   │◄──►│  ┌──────────┐ ┌────────┐ ┌────────────┐  │ │
│  │  Charts      │WS  │  │ Ingest   │►│Quality │►│ Transform  │  │ │
│  │  Live Logs   │    │  │ CSV/API/ │ │ Checks │ │ Enrich     │  │ │
│  │  Data Table  │    │  │ Stream   │ │ 5-dim  │ │ Dedupe     │  │ │
│  └──────────────┘    │  └──────────┘ └────────┘ │ Anomaly    │  │ │
│         │            │                           │ Aggregate  │  │ │
│         │            │                           └─────┬──────┘  │ │
│         ▼            │                                 ▼         │ │
│  ┌──────────────┐    │  ┌──────────────────────────────────────┐ │ │
│  │  FastAPI     │    │  │          Data Warehouse               │ │ │
│  │  REST API    │    │  │  SQLite (dev) / PostgreSQL (prod)     │ │ │
│  │  WebSocket   │◄───┤  │  processed_transactions              │ │ │
│  │  /api/...    │    │  │  pipeline_runs · pipeline_metrics    │ │ │
│  └──────────────┘    │  └──────────────────────────────────────┘ │ │
│                      └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **API** | FastAPI 0.115, Uvicorn, WebSockets |
| **Data Processing** | Python 3.12, Pandas-style algorithms, asyncio |
| **Database** | SQLite + aiosqlite (async), schema-forward design |
| **Frontend** | Vanilla JS/HTML/CSS, Chart.js, WebSocket API |
| **Infrastructure** | Docker, Docker Compose, Nginx reverse proxy |
| **CI/CD** | GitHub Actions (lint → test → docker build → security audit) |
| **Testing** | Pytest, pytest-asyncio, httpx (ASGI transport) |

---

## Features

### ETL Pipeline Engine

Three built-in pipelines, each with configurable sources:

| Pipeline | Source | Schedule | Records |
|---|---|---|---|
| E-Commerce Transactions | CSV file | Daily @ 02:00 | ~5,000/run |
| Orders API Sync | Paginated REST API | Every 30 min | ~2,000/run |
| Real-Time Event Stream | Kafka (simulated) | Continuous | ~3,000/run |

### Data Quality Framework

Every pipeline run executes 5 quality checks with weighted scoring:

| Check | Weight | Description |
|---|---|---|
| **Completeness** | 25% | Required fields are non-null |
| **Validity** | 25% | Values within expected domains |
| **Uniqueness** | 20% | No duplicate order IDs |
| **Timeliness** | 15% | Timestamps within 90-day window |
| **Consistency** | 15% | Cross-field logical consistency |

### Anomaly Detection

Statistical Z-score analysis flags orders where `|z| > 3.5` deviations from the mean — surfacing fraud, pricing errors, or data issues in real time.

### REST API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/pipelines` | List all pipelines with live stats |
| `GET` | `/api/pipelines/{id}` | Pipeline details + recent runs |
| `POST` | `/api/pipelines/{id}/trigger` | Trigger a run (supports dry_run) |
| `GET` | `/api/runs` | Run history with filtering |
| `GET` | `/api/runs/{run_id}` | Full run details + quality checks |
| `GET` | `/api/runs/{run_id}/logs` | Execution logs |
| `GET` | `/api/data/preview` | Browse processed records |
| `GET` | `/api/metrics` | Platform-wide aggregated metrics |
| `WS` | `/ws/runs/{run_id}` | Real-time pipeline events |
| `GET` | `/api/health` | Health check |

---

## Quick Start

### Option 1 — Docker Compose (recommended)

```bash
git clone https://github.com/anirudhdev/dataforge.git
cd dataforge
docker compose up --build
```

Open **http://localhost** for the dashboard, **http://localhost:8000/api/docs** for Swagger.

### Option 2 — Local Development

```bash
# Backend
cd backend
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# The frontend is served by FastAPI at http://localhost:8000
```

### Running Tests

```bash
cd backend
pytest tests/ -v --asyncio-mode=auto
```

---

## Project Structure

```
dataforge/
├── backend/
│   ├── app/
│   │   ├── main.py                    # FastAPI app, WebSocket endpoint
│   │   ├── models.py                  # Pydantic schemas
│   │   ├── database.py                # Async SQLite layer
│   │   ├── core/
│   │   │   └── connection_manager.py  # WebSocket connection pool
│   │   ├── pipeline/
│   │   │   ├── engine.py              # Pipeline orchestrator
│   │   │   ├── ingestion.py           # CSV / API / Stream connectors
│   │   │   ├── transformation.py      # Dedupe, enrich, anomaly detection
│   │   │   ├── quality.py             # 5-dimension DQ framework
│   │   │   └── loader.py              # Batch warehouse loader
│   │   └── api/
│   │       └── routes.py              # REST API endpoints
│   ├── tests/
│   │   ├── test_pipeline.py           # Unit tests for pipeline logic
│   │   └── test_api.py                # Integration tests for API
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── index.html                     # SPA dashboard
│   ├── styles.css                     # Dark theme UI system
│   └── app.js                         # WebSocket client + Chart.js
├── .github/
│   └── workflows/
│       └── ci.yml                     # Lint → Test → Docker → Security
├── docker-compose.yml
├── nginx.conf
└── .env.example
```

---

## Design Decisions

**Why SQLite (not PostgreSQL)?**
SQLite with async I/O via `aiosqlite` is sufficient for this demo and removes infrastructure dependencies. In production, swap `database.py` for an async SQLAlchemy engine pointed at PostgreSQL or Snowflake — the interface stays the same.

**Why WebSockets over polling?**
Polling has inherent latency and wastes connections. WebSockets provide sub-100ms event delivery, critical for the "live logs" UX. The `ConnectionManager` handles fan-out to multiple dashboard tabs simultaneously.

**Why vanilla JS (no React/Vue)?**
The frontend has no build step, making it trivially deployable anywhere. For a team project, the JS layer is architected with a clear state/render separation and could be migrated to React incrementally.

**Why async everywhere?**
Pipeline runs can take 10–30 seconds. Async Python (asyncio + aiosqlite + FastAPI) keeps the API responsive for concurrent users during runs, without threading complexity.

---

## Extending DataForge

Production extensions to discuss in interviews:

- **Replace ingestion**: Swap `CsvIngestion` for a real `KafkaConsumer` or `S3Reader`
- **Scale the warehouse**: Point `database.py` at BigQuery, Snowflake, or Redshift
- **Add orchestration**: Wrap `execute_pipeline()` with Airflow/Prefect DAGs
- **Observability**: Add OpenTelemetry tracing to each pipeline step
- **Schema registry**: Add Avro/Protobuf schema validation in the quality layer
- **Backfill support**: Add `date_range` parameter to ingestion sources

---

## License

MIT — built by [Anirudh](https://anirudhdev.com)
