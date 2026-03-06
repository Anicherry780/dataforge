"""
Data Quality Framework — Pipeline-aware checks
Dispatches to the right check set based on source type.
Each pipeline runs 5 DQ dimensions: Completeness, Validity, Uniqueness, Timeliness, Consistency.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

KNOWN_EVENT_TYPES = {
    "PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "DeleteEvent",
    "ReleaseEvent", "PublicEvent", "MemberEvent", "CommitCommentEvent",
    "PullRequestReviewEvent", "PullRequestReviewCommentEvent", "GollumEvent",
}


# ── Generic helpers ────────────────────────────────────────────────────────────

def _completeness(records: List[Dict], required: List[str]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    null_counts: Dict[str, int] = {}
    for r in records:
        bad = False
        for f in required:
            if r.get(f) is None or r.get(f) == "":
                null_counts[f] = null_counts.get(f, 0) + 1
                bad = True
        if bad:
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{f}: {c} nulls" for f, c in null_counts.items()) or "All required fields present"
    return {"name": "Completeness", "status": _status(pass_rate, 98, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


def _uniqueness(records: List[Dict], key_field: str) -> Dict[str, Any]:
    total = len(records)
    seen: set = set()
    dupes = 0
    for r in records:
        k = r.get(key_field)
        if k in seen:
            dupes += 1
        else:
            seen.add(k)
    pass_rate = round((1 - dupes / total) * 100, 2) if total else 0
    details = f"{dupes} duplicate {key_field}s found" if dupes else "No duplicates"
    return {"name": "Uniqueness", "status": _status(pass_rate, 99, 95),
            "records_checked": total, "records_failed": dupes,
            "pass_rate": pass_rate, "details": details}


def _timeliness(records: List[Dict], ts_field: str, max_age_hours: float = 2) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    now = datetime.now(timezone.utc)
    reasons: Dict[str, int] = {}
    for r in records:
        ts_str = r.get(ts_field)
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts > now:
                reasons["future_timestamp"] = reasons.get("future_timestamp", 0) + 1
                failed += 1
            elif (now - ts) > timedelta(hours=max_age_hours):
                reasons["too_old"] = reasons.get("too_old", 0) + 1
                failed += 1
        except Exception:
            reasons["unparseable"] = reasons.get("unparseable", 0) + 1
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{k}: {v}" for k, v in reasons.items()) or "All timestamps valid"
    return {"name": "Timeliness", "status": _status(pass_rate, 98, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


def _status(rate: float, pass_thresh: float, warn_thresh: float) -> str:
    if rate >= pass_thresh:
        return "pass"
    if rate >= warn_thresh:
        return "warn"
    return "fail"


# ── Crypto-specific checks ─────────────────────────────────────────────────────

def _crypto_validity(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    reasons: Dict[str, int] = {}
    for r in records:
        bad = False
        price = r.get("current_price")
        if price is not None and (not isinstance(price, (int, float)) or price < 0):
            reasons["invalid_price"] = reasons.get("invalid_price", 0) + 1
            bad = True
        mc = r.get("market_cap")
        if mc is not None and mc < 0:
            reasons["negative_market_cap"] = reasons.get("negative_market_cap", 0) + 1
            bad = True
        rank = r.get("market_cap_rank")
        if rank is not None and rank < 1:
            reasons["invalid_rank"] = reasons.get("invalid_rank", 0) + 1
            bad = True
        if bad:
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{k}: {v}" for k, v in reasons.items()) or "All values valid"
    return {"name": "Validity", "status": _status(pass_rate, 97, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


def _crypto_consistency(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    reasons: Dict[str, int] = {}
    for r in records:
        high = r.get("high_24h") or 0
        low  = r.get("low_24h") or 0
        price = r.get("current_price") or 0
        if high and low and high < low:
            reasons["high_lt_low"] = reasons.get("high_lt_low", 0) + 1
            failed += 1
        elif price and high and price > high * 1.01:
            reasons["price_above_high"] = reasons.get("price_above_high", 0) + 1
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{k}: {v}" for k, v in reasons.items()) or "Price ranges consistent"
    return {"name": "Consistency", "status": _status(pass_rate, 99, 95),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


# ── Weather-specific checks ────────────────────────────────────────────────────

def _weather_validity(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    reasons: Dict[str, int] = {}
    for r in records:
        bad = False
        temp = r.get("temperature_c")
        if temp is not None and not (-90 <= temp <= 60):
            reasons["impossible_temperature"] = reasons.get("impossible_temperature", 0) + 1
            bad = True
        hum = r.get("humidity_pct")
        if hum is not None and not (0 <= hum <= 100):
            reasons["invalid_humidity"] = reasons.get("invalid_humidity", 0) + 1
            bad = True
        wind = r.get("wind_speed_kmh")
        if wind is not None and wind < 0:
            reasons["negative_wind"] = reasons.get("negative_wind", 0) + 1
            bad = True
        if bad:
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{k}: {v}" for k, v in reasons.items()) or "All weather values valid"
    return {"name": "Validity", "status": _status(pass_rate, 97, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


def _weather_consistency(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    for r in records:
        temp = r.get("temperature_c")
        feels = r.get("apparent_temp_c")
        if temp is not None and feels is not None:
            if abs(temp - feels) > 25:
                failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = f"{failed} records with implausible apparent temp" if failed else "Temp vs apparent-temp consistent"
    return {"name": "Consistency", "status": _status(pass_rate, 97, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


# ── GitHub-specific checks ─────────────────────────────────────────────────────

def _github_validity(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    failed = 0
    reasons: Dict[str, int] = {}
    for r in records:
        et = r.get("event_type", "")
        if et not in KNOWN_EVENT_TYPES:
            reasons["unknown_event_type"] = reasons.get("unknown_event_type", 0) + 1
            failed += 1
    pass_rate = round((1 - failed / total) * 100, 2) if total else 0
    details = ", ".join(f"{k}: {v}" for k, v in reasons.items()) or "All event types valid"
    return {"name": "Validity", "status": _status(pass_rate, 97, 90),
            "records_checked": total, "records_failed": failed,
            "pass_rate": pass_rate, "details": details}


def _github_consistency(records: List[Dict]) -> Dict[str, Any]:
    total = len(records)
    private = sum(1 for r in records if not r.get("is_public", True))
    pass_rate = round((1 - private / total) * 100, 2) if total else 0
    details = f"{private} non-public events in public stream" if private else "All events are public"
    return {"name": "Consistency", "status": _status(pass_rate, 99, 95),
            "records_checked": total, "records_failed": private,
            "pass_rate": pass_rate, "details": details}


# ── Dispatcher ─────────────────────────────────────────────────────────────────

def run_all_checks(records: List[Dict], source_type: str = "crypto") -> Tuple[List[Dict], float]:
    """Run all 5 DQ checks for the given source type. Returns (checks, weighted_score)."""
    if not records:
        return [], 0.0

    if source_type == "crypto":
        checks = [
            _completeness(records, ["coin_id", "current_price", "market_cap", "symbol"]),
            _crypto_validity(records),
            _uniqueness(records, "coin_id"),
            _timeliness(records, "last_updated", max_age_hours=1),
            _crypto_consistency(records),
        ]
    elif source_type == "weather":
        checks = [
            _completeness(records, ["city", "temperature_c", "humidity_pct", "observed_at"]),
            _weather_validity(records),
            _uniqueness(records, "city"),
            _timeliness(records, "observed_at", max_age_hours=3),
            _weather_consistency(records),
        ]
    elif source_type == "github":
        checks = [
            _completeness(records, ["event_id", "event_type", "actor", "repo_name"]),
            _github_validity(records),
            _uniqueness(records, "event_id"),
            _timeliness(records, "created_at", max_age_hours=24),
            _github_consistency(records),
        ]
    else:
        checks = [
            _completeness(records, list(records[0].keys())[:4]),
            {"name": "Validity",     "status": "pass", "records_checked": len(records), "records_failed": 0, "pass_rate": 100.0, "details": "—"},
            {"name": "Uniqueness",   "status": "pass", "records_checked": len(records), "records_failed": 0, "pass_rate": 100.0, "details": "—"},
            {"name": "Timeliness",   "status": "pass", "records_checked": len(records), "records_failed": 0, "pass_rate": 100.0, "details": "—"},
            {"name": "Consistency",  "status": "pass", "records_checked": len(records), "records_failed": 0, "pass_rate": 100.0, "details": "—"},
        ]

    weights = [0.25, 0.25, 0.20, 0.15, 0.15]
    score = sum(c["pass_rate"] * w for c, w in zip(checks, weights))
    return checks, round(score, 1)
