import aiosqlite
import json
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "dataforge.db"))).resolve()


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                started_at TEXT NOT NULL,
                completed_at TEXT,
                duration_ms INTEGER,
                records_ingested INTEGER DEFAULT 0,
                records_transformed INTEGER DEFAULT 0,
                records_loaded INTEGER DEFAULT 0,
                quality_score REAL DEFAULT 0.0,
                steps TEXT DEFAULT '[]',
                quality_checks TEXT DEFAULT '[]',
                logs TEXT DEFAULT '[]',
                error_message TEXT
            )
        """)

        # ── Crypto records ────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS crypto_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                coin_id TEXT,
                symbol TEXT,
                name TEXT,
                current_price REAL,
                market_cap REAL,
                market_cap_rank INTEGER,
                total_volume REAL,
                price_change_pct_24h REAL,
                volatility_pct REAL,
                volume_mc_ratio REAL,
                momentum REAL,
                signal TEXT,
                is_anomaly INTEGER DEFAULT 0,
                anomaly_reason TEXT,
                processed_at TEXT
            )
        """)

        # ── Weather records ────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS weather_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                city TEXT,
                latitude REAL,
                longitude REAL,
                temperature_c REAL,
                apparent_temp_c REAL,
                humidity_pct REAL,
                precipitation_mm REAL,
                wind_speed_kmh REAL,
                condition TEXT,
                heat_index_c REAL,
                is_extreme INTEGER DEFAULT 0,
                is_anomaly INTEGER DEFAULT 0,
                anomaly_reason TEXT,
                observed_at TEXT,
                processed_at TEXT
            )
        """)

        # ── GitHub records ─────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS github_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event_id TEXT,
                event_type TEXT,
                actor TEXT,
                repo_name TEXT,
                action TEXT,
                push_size INTEGER,
                repo_event_count INTEGER,
                is_trending_repo INTEGER DEFAULT 0,
                is_anomaly INTEGER DEFAULT 0,
                anomaly_reason TEXT,
                created_at TEXT,
                processed_at TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """)

        # Indexes
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_runs_pipeline_id ON pipeline_runs(pipeline_id)",
            "CREATE INDEX IF NOT EXISTS idx_crypto_run_id ON crypto_records(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_weather_run_id ON weather_records(run_id)",
            "CREATE INDEX IF NOT EXISTS idx_github_run_id ON github_records(run_id)",
        ]:
            await db.execute(stmt)
        await db.commit()


async def save_run(run_data: Dict[str, Any]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO pipeline_runs
            (run_id, pipeline_id, pipeline_name, status, started_at,
             completed_at, duration_ms, records_ingested, records_transformed,
             records_loaded, quality_score, steps, quality_checks, logs, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_data["run_id"],
            run_data["pipeline_id"],
            run_data["pipeline_name"],
            run_data["status"],
            run_data["started_at"],
            run_data.get("completed_at"),
            run_data.get("duration_ms"),
            run_data.get("records_ingested", 0),
            run_data.get("records_transformed", 0),
            run_data.get("records_loaded", 0),
            run_data.get("quality_score", 0.0),
            json.dumps(run_data.get("steps", [])),
            json.dumps(run_data.get("quality_checks", [])),
            json.dumps(run_data.get("logs", [])),
            run_data.get("error_message"),
        ))
        await db.commit()


async def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                data = dict(row)
                data["steps"] = json.loads(data["steps"])
                data["quality_checks"] = json.loads(data["quality_checks"])
                data["logs"] = json.loads(data["logs"])
                return data
    return None


async def get_runs_by_pipeline(pipeline_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM pipeline_runs WHERE pipeline_id = ? ORDER BY started_at DESC LIMIT ?",
            (pipeline_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                data = dict(row)
                data["steps"] = json.loads(data["steps"])
                data["quality_checks"] = json.loads(data["quality_checks"])
                data["logs"] = json.loads(data["logs"])
                result.append(data)
            return result


async def get_all_runs(limit: int = 50) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                data = dict(row)
                data["steps"] = json.loads(data["steps"])
                data["quality_checks"] = json.loads(data["quality_checks"])
                data["logs"] = json.loads(data["logs"])
                result.append(data)
            return result


# ── Per-pipeline record savers ─────────────────────────────────────────────────

async def save_crypto_records(run_id: str, records: List[Dict[str, Any]]):
    if not records:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany("""
            INSERT INTO crypto_records
            (run_id, coin_id, symbol, name, current_price, market_cap, market_cap_rank,
             total_volume, price_change_pct_24h, volatility_pct, volume_mc_ratio,
             momentum, signal, is_anomaly, anomaly_reason, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                run_id,
                r.get("coin_id"), r.get("symbol"), r.get("name"),
                r.get("current_price"), r.get("market_cap"), r.get("market_cap_rank"),
                r.get("total_volume"), r.get("price_change_pct_24h"),
                r.get("volatility_pct"), r.get("volume_mc_ratio"),
                r.get("momentum"), r.get("signal"),
                1 if r.get("is_anomaly") else 0,
                r.get("anomaly_reason", ""), r.get("processed_at"),
            )
            for r in records
        ])
        await db.commit()


async def save_weather_records(run_id: str, records: List[Dict[str, Any]]):
    if not records:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany("""
            INSERT INTO weather_records
            (run_id, city, latitude, longitude, temperature_c, apparent_temp_c,
             humidity_pct, precipitation_mm, wind_speed_kmh, condition,
             heat_index_c, is_extreme, is_anomaly, anomaly_reason, observed_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                run_id,
                r.get("city"), r.get("latitude"), r.get("longitude"),
                r.get("temperature_c"), r.get("apparent_temp_c"),
                r.get("humidity_pct"), r.get("precipitation_mm"),
                r.get("wind_speed_kmh"), r.get("condition"),
                r.get("heat_index_c"),
                1 if r.get("is_extreme") else 0,
                1 if r.get("is_anomaly") else 0,
                r.get("anomaly_reason", ""), r.get("observed_at"), r.get("processed_at"),
            )
            for r in records
        ])
        await db.commit()


async def save_github_records(run_id: str, records: List[Dict[str, Any]]):
    if not records:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany("""
            INSERT INTO github_records
            (run_id, event_id, event_type, actor, repo_name, action,
             push_size, repo_event_count, is_trending_repo,
             is_anomaly, anomaly_reason, created_at, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                run_id,
                r.get("event_id"), r.get("event_type"), r.get("actor"),
                r.get("repo_name"), r.get("action"),
                r.get("push_size"), r.get("repo_event_count"),
                1 if r.get("is_trending_repo") else 0,
                1 if r.get("is_anomaly") else 0,
                r.get("anomaly_reason", ""), r.get("created_at"), r.get("processed_at"),
            )
            for r in records
        ])
        await db.commit()


async def save_records(run_id: str, pipeline_id: str, records: List[Dict[str, Any]]):
    """Dispatcher — routes to the correct table based on pipeline_id."""
    if pipeline_id == "crypto-market":
        await save_crypto_records(run_id, records)
    elif pipeline_id == "weather-observations":
        await save_weather_records(run_id, records)
    elif pipeline_id == "github-events":
        await save_github_records(run_id, records)


async def get_data_preview(
    run_id: Optional[str] = None,
    source_type: Optional[str] = None,
    limit: int = 100,
) -> List[Dict]:
    """Return latest records. Queries the right table based on source_type."""
    table_map = {
        "crypto":  "crypto_records",
        "weather": "weather_records",
        "github":  "github_records",
    }

    # Determine which tables to query
    if source_type and source_type in table_map:
        tables = [table_map[source_type]]
    else:
        tables = list(table_map.values())

    all_records: List[Dict] = []
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        for table in tables:
            if run_id:
                q = f"SELECT * FROM {table} WHERE run_id = ? ORDER BY id DESC LIMIT ?"
                p = (run_id, limit)
            else:
                q = f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?"
                p = (limit,)
            async with db.execute(q, p) as cursor:
                rows = await cursor.fetchall()
                all_records.extend(dict(r) for r in rows)

    # Sort by id descending (id is local to each table, so approximate)
    all_records.sort(key=lambda r: r.get("id", 0), reverse=True)
    return all_records[:limit]


async def get_metrics_summary() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute(
            "SELECT COUNT(*) as total, SUM(records_loaded) as total_records, "
            "AVG(CASE WHEN status='success' THEN 1.0 ELSE 0.0 END) as success_rate, "
            "AVG(quality_score) as avg_quality "
            "FROM pipeline_runs"
        ) as cur:
            summary = dict(await cur.fetchone())

        async with db.execute("""
            SELECT DATE(started_at) as day, SUM(records_loaded) as records
            FROM pipeline_runs
            WHERE status = 'success'
            GROUP BY DATE(started_at)
            ORDER BY day DESC
            LIMIT 14
        """) as cur:
            by_day = [dict(r) for r in await cur.fetchall()]

        async with db.execute("""
            SELECT pipeline_name, AVG(quality_score) as avg_quality,
                   COUNT(*) as runs, SUM(records_loaded) as total_records
            FROM pipeline_runs
            GROUP BY pipeline_id
        """) as cur:
            by_pipeline = [dict(r) for r in await cur.fetchall()]

        async with db.execute("""
            SELECT pipeline_name,
                   AVG(duration_ms) as avg_duration,
                   MIN(duration_ms) as min_duration,
                   MAX(duration_ms) as max_duration
            FROM pipeline_runs
            WHERE status = 'success'
            GROUP BY pipeline_id
        """) as cur:
            performance = [dict(r) for r in await cur.fetchall()]

        return {
            "total_pipeline_runs": summary.get("total") or 0,
            "total_records_processed": summary.get("total_records") or 0,
            "success_rate": round((summary.get("success_rate") or 0) * 100, 1),
            "avg_quality_score": round(summary.get("avg_quality") or 0, 1),
            "records_by_day": by_day,
            "quality_by_pipeline": by_pipeline,
            "pipeline_performance": performance,
        }
