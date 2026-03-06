"""
Data Ingestion Module — Real API Connectors (no API keys required)

  CryptoMarketIngestion  → CoinCap API    — top-N coins, live market data
  WeatherIngestion       → Open-Meteo API  — current conditions, 10 global cities
  GitHubEventsIngestion  → GitHub API      — public developer event stream
"""
import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx

ProgressCb = Optional[Callable[[int, int, str], Awaitable[None]]]

CITIES = [
    {"name": "New York",   "lat": 40.7128,  "lon": -74.0060},
    {"name": "London",     "lat": 51.5074,  "lon": -0.1278},
    {"name": "Tokyo",      "lat": 35.6762,  "lon": 139.6503},
    {"name": "Sydney",     "lat": -33.8688, "lon": 151.2093},
    {"name": "Mumbai",     "lat": 19.0760,  "lon": 72.8777},
    {"name": "Berlin",     "lat": 52.5200,  "lon": 13.4050},
    {"name": "Singapore",  "lat": 1.3521,   "lon": 103.8198},
    {"name": "Dubai",      "lat": 25.2048,  "lon": 55.2708},
    {"name": "São Paulo",  "lat": -23.5505, "lon": -46.6333},
    {"name": "Toronto",    "lat": 43.6532,  "lon": -79.3832},
]

WMO_CONDITIONS = {
    0: "Clear Sky", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy Fog", 51: "Light Drizzle", 53: "Moderate Drizzle",
    55: "Dense Drizzle", 61: "Slight Rain", 63: "Moderate Rain", 65: "Heavy Rain",
    71: "Slight Snow", 73: "Moderate Snow", 75: "Heavy Snow",
    80: "Showers", 81: "Moderate Showers", 82: "Violent Showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ Hail", 99: "Thunderstorm w/ Heavy Hail",
}


class CryptoMarketIngestion:
    """Fetches live crypto market data from CoinCap (free, no auth, cloud-friendly)."""
    BASE_URL = "https://api.coincap.io/v2/assets"

    def __init__(self, top_n: int = 50):
        self.top_n = min(top_n, 200)

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(
                    self.BASE_URL,
                    params={"limit": self.top_n},
                )
                resp.raise_for_status()
                data = resp.json().get("data", [])
            except Exception:
                # Return empty on failure
                return records

            for i, coin in enumerate(data):
                price = float(coin.get("priceUsd") or 0)
                mc = float(coin.get("marketCapUsd") or 0)
                vol = float(coin.get("volumeUsd24Hr") or 0)
                chg24 = float(coin.get("changePercent24Hr") or 0)
                supply = float(coin.get("supply") or 0)
                vwap = float(coin.get("vwap24Hr") or 0)

                # Approximate high/low from vwap and change
                high_24h = max(price, vwap * 1.02) if vwap else price
                low_24h = min(price, vwap * 0.98) if vwap else price

                records.append({
                    "coin_id":              coin.get("id"),
                    "symbol":               (coin.get("symbol") or "").upper(),
                    "name":                 coin.get("name"),
                    "current_price":        round(price, 6),
                    "market_cap":           round(mc, 2),
                    "market_cap_rank":      int(coin.get("rank") or 0),
                    "total_volume":         round(vol, 2),
                    "high_24h":             round(high_24h, 6),
                    "low_24h":              round(low_24h, 6),
                    "price_change_24h":     round(price * chg24 / 100, 6),
                    "price_change_pct_1h":  None,  # not available in CoinCap
                    "price_change_pct_24h": round(chg24, 2),
                    "price_change_pct_7d":  None,  # not available in CoinCap
                    "circulating_supply":   round(supply, 2),
                    "ath":                  None,   # not available in CoinCap
                    "ath_change_pct":       None,
                    "last_updated":         fetched_at,
                    "source":               "coincap",
                    "fetched_at":           fetched_at,
                })

                if on_progress and (i + 1) % 25 == 0:
                    await on_progress(i + 1, len(data), f"ingestion:batch_{i + 1}")

        if on_progress:
            await on_progress(len(records), self.top_n, "ingestion:done")
        return records


class WeatherIngestion:
    """Fetches current weather for global cities via Open-Meteo (free, no auth)."""
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, cities: Optional[List[Dict]] = None):
        self.cities = cities or CITIES

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        async with httpx.AsyncClient(timeout=20) as client:
            for i, city in enumerate(self.cities):
                params = {
                    "latitude":  city["lat"],
                    "longitude": city["lon"],
                    "current":   ("temperature_2m,relative_humidity_2m,apparent_temperature,"
                                  "precipitation,wind_speed_10m,wind_direction_10m,"
                                  "weather_code,cloud_cover,pressure_msl"),
                    "timezone": "UTC",
                }
                try:
                    resp = await client.get(self.BASE_URL, params=params)
                    resp.raise_for_status()
                    cur = resp.json().get("current", {})
                    records.append({
                        "city":             city["name"],
                        "latitude":         city["lat"],
                        "longitude":        city["lon"],
                        "temperature_c":    cur.get("temperature_2m"),
                        "apparent_temp_c":  cur.get("apparent_temperature"),
                        "humidity_pct":     cur.get("relative_humidity_2m"),
                        "precipitation_mm": cur.get("precipitation"),
                        "wind_speed_kmh":   cur.get("wind_speed_10m"),
                        "wind_direction":   cur.get("wind_direction_10m"),
                        "cloud_cover_pct":  cur.get("cloud_cover"),
                        "pressure_hpa":     cur.get("pressure_msl"),
                        "weather_code":     cur.get("weather_code"),
                        "condition":        WMO_CONDITIONS.get(cur.get("weather_code", -1), "Unknown"),
                        "observed_at":      cur.get("time"),
                        "source":           "open-meteo",
                        "fetched_at":       fetched_at,
                    })
                except Exception:
                    pass

                if on_progress:
                    await on_progress(i + 1, len(self.cities), f"ingestion:{city['name']}")
                await asyncio.sleep(0.15)

        return records

class GitHubEventsIngestion:
    """Streams public GitHub events. Uses GITHUB_TOKEN env var for higher rate limits (5000/hr vs 60/hr)."""
    BASE_URL = "https://api.github.com/events"

    def __init__(self, max_pages: int = 3):
        self.max_pages = max_pages
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataForge-ETL/1.0",
        }
        # Use token if available — 5,000 req/hr vs 60 req/hr
        token = os.environ.get("GITHUB_TOKEN", "")
        if token:
            self.headers["Authorization"] = f"token {token}"

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(1, self.max_pages + 1):
                # Retry with backoff on rate limit
                resp = None
                for attempt in range(3):
                    try:
                        resp = await client.get(
                            self.BASE_URL, headers=self.headers,
                            params={"per_page": 30, "page": page},
                        )
                        if resp.status_code in (429, 403):
                            wait = 5 * (2 ** attempt)
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        break
                    except Exception:
                        if attempt < 2:
                            await asyncio.sleep(5)
                        else:
                            resp = None
                            break

                if resp is None or resp.status_code != 200:
                    break

                events = resp.json()
                if not events:
                    break

                for ev in events:
                    payload = ev.get("payload", {}) or {}
                    pr = payload.get("pull_request") or {}
                    records.append({
                        "event_id":     ev.get("id"),
                        "event_type":   ev.get("type"),
                        "actor":        (ev.get("actor") or {}).get("login"),
                        "repo_name":    (ev.get("repo") or {}).get("name"),
                        "repo_id":      (ev.get("repo") or {}).get("id"),
                        "is_public":    ev.get("public", True),
                        "action":       payload.get("action"),
                        "ref_type":     payload.get("ref_type"),
                        "push_size":    payload.get("size"),
                        "pr_number":    pr.get("number"),
                        "pr_merged":    pr.get("merged"),
                        "pr_additions": pr.get("additions"),
                        "pr_deletions": pr.get("deletions"),
                        "created_at":   ev.get("created_at"),
                        "source":       "github",
                        "fetched_at":   fetched_at,
                    })

                if on_progress:
                    await on_progress(len(records), self.max_pages * 30, f"ingestion:page_{page}")
                await asyncio.sleep(1.5)  # generous delay for free-tier

        return records


_ORDER_COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"]
_ORDER_STATUSES  = ["completed", "pending", "cancelled", "refunded", "processing"]
_ORDER_CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Food"]


class CsvIngestion:
    """Simulates reading order records from a CSV file (synthetic data for testing)."""

    def __init__(self, num_records: int = 100):
        self.num_records = num_records

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        import random
        now = datetime.now(timezone.utc).isoformat()
        records = []
        for i in range(self.num_records):
            records.append({
                "order_id":   f"ORD-{i:06d}",
                "user_id":    f"USR-{random.randint(1, 1000):04d}",
                "product_id": f"PROD-{random.randint(1, 500):04d}",
                "category":   random.choice(_ORDER_CATEGORIES),
                "amount":     round(random.uniform(5.0, 500.0), 2),
                "quantity":   random.randint(1, 10),
                "country":    random.choice(_ORDER_COUNTRIES),
                "status":     random.choice(_ORDER_STATUSES),
                "created_at": now,
                "source":     "csv",
            })
        return records


class ApiIngestion:
    """Simulates fetching order records from a paginated API (synthetic data for testing)."""

    def __init__(self, num_records: int = 200, page_size: int = 50):
        self.num_records = num_records
        self.page_size = page_size

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        import random
        now = datetime.now(timezone.utc).isoformat()
        records: List[Dict[str, Any]] = []
        pages = (self.num_records + self.page_size - 1) // self.page_size
        for page in range(pages):
            batch_size = min(self.page_size, self.num_records - len(records))
            for j in range(batch_size):
                i = page * self.page_size + j
                records.append({
                    "order_id":   f"ORD-API-{i:06d}",
                    "user_id":    f"USR-{random.randint(1, 1000):04d}",
                    "product_id": f"PROD-{random.randint(1, 500):04d}",
                    "category":   random.choice(_ORDER_CATEGORIES),
                    "amount":     round(random.uniform(5.0, 500.0), 2),
                    "quantity":   random.randint(1, 10),
                    "country":    random.choice(_ORDER_COUNTRIES),
                    "status":     random.choice(_ORDER_STATUSES),
                    "created_at": now,
                    "source":     "api",
                })
            if on_progress:
                await on_progress(len(records), self.num_records, f"ingestion:page_{page + 1}")
            await asyncio.sleep(0)
        return records


def get_ingestion_source(source_type: str, source_config: Optional[Dict] = None):
    """Factory — returns the right connector for a pipeline's source_type."""
    cfg = source_config or {}
    factories = {
        "crypto":  lambda: CryptoMarketIngestion(**cfg),
        "weather": lambda: WeatherIngestion(),
        "github":  lambda: GitHubEventsIngestion(**cfg),
    }
    if source_type not in factories:
        raise ValueError(f"Unknown source_type: '{source_type}'")
    return factories[source_type]()
