"""
Transformation Module — Pipeline-aware enrichment and anomaly detection.
Each pipeline source gets domain-specific transforms after generic dedup/cast.
"""
import asyncio
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import List, Dict, Any, Tuple


# ── Generic ────────────────────────────────────────────────────────────────────

def deduplicate(records: List[Dict], key: str) -> Tuple[List[Dict], int]:
    seen: set = set()
    unique = []
    for r in records:
        k = r.get(key)
        if k not in seen:
            seen.add(k)
            unique.append(r)
    return unique, len(records) - len(unique)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Crypto transforms ──────────────────────────────────────────────────────────

def _enrich_crypto(records: List[Dict]) -> Tuple[List[Dict], int]:
    amounts = [r["current_price"] for r in records if (r.get("current_price") or 0) > 0]
    mu = mean(amounts) if len(amounts) > 1 else 0
    sigma = stdev(amounts) if len(amounts) > 1 else 1

    anomalies = 0
    for r in records:
        price  = r.get("current_price") or 0
        high   = r.get("high_24h") or price
        low    = r.get("low_24h") or price
        volume = r.get("total_volume") or 0
        mc     = r.get("market_cap") or 1

        # Volatility: daily range / current price
        r["volatility_pct"] = round((high - low) / price * 100, 2) if price else 0

        # Volume/market-cap ratio — signals unusual activity
        r["volume_mc_ratio"] = round(volume / mc, 4) if mc else 0

        # Momentum: composite of 1h + 24h change
        p1h  = r.get("price_change_pct_1h") or 0
        p24h = r.get("price_change_pct_24h") or 0
        r["momentum"] = round(p1h * 0.6 + p24h * 0.4, 2)

        # Signal
        r["signal"] = "bullish" if r["momentum"] > 2 else ("bearish" if r["momentum"] < -2 else "neutral")

        # Z-score anomaly on price
        z = abs((price - mu) / sigma) if sigma else 0
        r["is_anomaly"] = z > 3.5 or abs(p24h) > 30
        r["anomaly_reason"] = (
            f"z-score={z:.1f}" if z > 3.5 else
            f"24h change={p24h:.1f}%" if abs(p24h) > 30 else ""
        )
        r["processed_at"] = _now()
        if r["is_anomaly"]:
            anomalies += 1

    return records, anomalies


def _aggregate_crypto(records: List[Dict]) -> Dict[str, Any]:
    prices = [r.get("current_price") or 0 for r in records if r.get("current_price")]
    volumes = [r.get("total_volume") or 0 for r in records]
    bullish = sum(1 for r in records if r.get("signal") == "bullish")
    bearish = sum(1 for r in records if r.get("signal") == "bearish")
    return {
        "total_coins":     len(records),
        "total_volume_usd": sum(volumes),
        "avg_price_usd":   round(mean(prices), 2) if prices else 0,
        "avg_volatility":  round(mean(r.get("volatility_pct", 0) for r in records), 2),
        "bullish_signals": bullish,
        "bearish_signals": bearish,
        "neutral_signals": len(records) - bullish - bearish,
        "anomalies_detected": sum(1 for r in records if r.get("is_anomaly")),
    }


# ── Weather transforms ─────────────────────────────────────────────────────────

def _enrich_weather(records: List[Dict]) -> Tuple[List[Dict], int]:
    anomalies = 0
    for r in records:
        temp = r.get("temperature_c") or 0
        hum  = r.get("humidity_pct") or 0

        # Heat index (simplified Steadman formula, valid > 27°C)
        if temp >= 27 and hum >= 40:
            hi = (-8.78469475556 + 1.61139411 * temp + 2.33854883889 * hum
                  - 0.14611605 * temp * hum - 0.012308094 * temp**2
                  - 0.0164248277778 * hum**2 + 0.002211732 * temp**2 * hum
                  + 0.00072546 * temp * hum**2 - 0.000003582 * temp**2 * hum**2)
            r["heat_index_c"] = round(hi, 1)
        else:
            r["heat_index_c"] = temp

        # Extreme flag
        r["is_extreme"] = (
            temp > 40 or temp < -20 or
            (r.get("wind_speed_kmh") or 0) > 100 or
            (r.get("precipitation_mm") or 0) > 50
        )
        r["is_anomaly"] = r["is_extreme"]
        r["anomaly_reason"] = "extreme weather conditions" if r["is_extreme"] else ""
        r["processed_at"] = _now()
        if r["is_anomaly"]:
            anomalies += 1

    return records, anomalies


def _aggregate_weather(records: List[Dict]) -> Dict[str, Any]:
    temps = [r.get("temperature_c") for r in records if r.get("temperature_c") is not None]
    return {
        "cities_observed": len(records),
        "avg_temp_c":      round(mean(temps), 1) if temps else 0,
        "min_temp_c":      min(temps) if temps else 0,
        "max_temp_c":      max(temps) if temps else 0,
        "extreme_events":  sum(1 for r in records if r.get("is_extreme")),
        "conditions":      {r["city"]: r.get("condition", "—") for r in records},
        "anomalies_detected": sum(1 for r in records if r.get("is_anomaly")),
    }


# ── GitHub transforms ──────────────────────────────────────────────────────────

def _enrich_github(records: List[Dict]) -> Tuple[List[Dict], int]:
    # Count events per repo to find trending repos
    repo_counts: Dict[str, int] = {}
    for r in records:
        rn = r.get("repo_name") or "unknown"
        repo_counts[rn] = repo_counts.get(rn, 0) + 1

    # Top repos (appearing 3+ times = trending in this window)
    trending = {repo for repo, cnt in repo_counts.items() if cnt >= 3}

    anomalies = 0
    for r in records:
        rn = r.get("repo_name") or "unknown"
        r["repo_event_count"] = repo_counts.get(rn, 1)
        r["is_trending_repo"] = rn in trending

        # Flag bots and unusually large pushes
        actor = r.get("actor") or ""
        push_size = r.get("push_size") or 0
        r["is_anomaly"] = actor.endswith("[bot]") or push_size > 500
        r["anomaly_reason"] = (
            "bot actor" if actor.endswith("[bot]") else
            f"large push ({push_size} commits)" if push_size > 500 else ""
        )
        r["processed_at"] = _now()
        if r["is_anomaly"]:
            anomalies += 1

    return records, anomalies


def _aggregate_github(records: List[Dict]) -> Dict[str, Any]:
    by_type: Dict[str, int] = {}
    for r in records:
        et = r.get("event_type") or "Unknown"
        by_type[et] = by_type.get(et, 0) + 1
    return {
        "total_events":      len(records),
        "unique_actors":     len({r.get("actor") for r in records}),
        "unique_repos":      len({r.get("repo_name") for r in records}),
        "events_by_type":    dict(sorted(by_type.items(), key=lambda x: -x[1])),
        "trending_repos":    len({r["repo_name"] for r in records if r.get("is_trending_repo")}),
        "anomalies_detected": sum(1 for r in records if r.get("is_anomaly")),
    }


# ── Main entry point ───────────────────────────────────────────────────────────

async def transform(
    records: List[Dict[str, Any]],
    source_type: str = "crypto",
    on_progress=None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Full transformation pipeline (source-aware):
      1. Deduplication
      2. Domain-specific enrichment + anomaly detection
      3. Aggregation
    """
    key_map = {"crypto": "coin_id", "weather": "city", "github": "event_id"}
    key = key_map.get(source_type, "id")

    records, dupes = deduplicate(records, key)
    if on_progress:
        await on_progress(1, 3, f"transform:dedup ({dupes} removed)")
    await asyncio.sleep(0.05)

    if source_type == "crypto":
        records, anomalies = _enrich_crypto(records)
        aggregations = _aggregate_crypto(records)
    elif source_type == "weather":
        records, anomalies = _enrich_weather(records)
        aggregations = _aggregate_weather(records)
    elif source_type == "github":
        records, anomalies = _enrich_github(records)
        aggregations = _aggregate_github(records)
    else:
        for r in records:
            r["is_anomaly"] = False
            r["anomaly_reason"] = ""
            r["processed_at"] = _now()
        anomalies = 0
        aggregations = {"total_records": len(records), "anomalies_detected": 0}

    if on_progress:
        await on_progress(2, 3, f"transform:enrich ({anomalies} anomalies)")
    await asyncio.sleep(0.05)

    if on_progress:
        await on_progress(3, 3, "transform:aggregate")
    await asyncio.sleep(0.05)

    return records, aggregations
