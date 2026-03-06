"""
Data Ingestion Module — Real API Connectors (no API keys required)

  CryptoMarketIngestion  → CoinGecko API   — top-N coins, live market data
  WeatherIngestion       → Open-Meteo API  — current conditions, 10 global cities
  GitHubEventsIngestion  → GitHub API      — public developer event stream
"""
import asyncio
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
    """Fetches live crypto market data from CoinGecko (free, no auth)."""
    BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"

    def __init__(self, top_n: int = 100):
        self.top_n = min(top_n, 250)

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        per_page = 100
        pages = (self.top_n + per_page - 1) // per_page
        fetched_at = datetime.now(timezone.utc).isoformat()

        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(1, pages + 1):
                params = {
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": min(per_page, self.top_n - len(records)),
                    "page": page,
                    "sparkline": "false",
                    "price_change_percentage": "1h,24h,7d",
                }
                resp = await client.get(self.BASE_URL, params=params)
                resp.raise_for_status()

                for coin in resp.json():
                    records.append({
                        "coin_id":              coin.get("id"),
                        "symbol":               (coin.get("symbol") or "").upper(),
                        "name":                 coin.get("name"),
                        "current_price":        coin.get("current_price"),
                        "market_cap":           coin.get("market_cap"),
                        "market_cap_rank":      coin.get("market_cap_rank"),
                        "total_volume":         coin.get("total_volume"),
                        "high_24h":             coin.get("high_24h"),
                        "low_24h":              coin.get("low_24h"),
                        "price_change_24h":     coin.get("price_change_24h"),
                        "price_change_pct_1h":  coin.get("price_change_percentage_1h_in_currency"),
                        "price_change_pct_24h": coin.get("price_change_percentage_24h"),
                        "price_change_pct_7d":  coin.get("price_change_percentage_7d_in_currency"),
                        "circulating_supply":   coin.get("circulating_supply"),
                        "ath":                  coin.get("ath"),
                        "ath_change_pct":       coin.get("ath_change_percentage"),
                        "last_updated":         coin.get("last_updated"),
                        "source":               "coingecko",
                        "fetched_at":           fetched_at,
                    })

                if on_progress:
                    await on_progress(len(records), self.top_n, f"ingestion:page_{page}")
                if page < pages:
                    await asyncio.sleep(1.5)  # free-tier rate limit

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
    """Streams public GitHub events (pushes, PRs, issues). No auth, 60 req/hr free."""
    BASE_URL = "https://api.github.com/events"
    HEADERS  = {"Accept": "application/vnd.github.v3+json", "User-Agent": "DataForge-ETL/1.0"}

    def __init__(self, max_pages: int = 5):
        self.max_pages = max_pages

    async def fetch(self, on_progress: ProgressCb = None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        fetched_at = datetime.now(timezone.utc).isoformat()

        async with httpx.AsyncClient(timeout=20) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    resp = await client.get(
                        self.BASE_URL, headers=self.HEADERS,
                        params={"per_page": 30, "page": page},
                    )
                    resp.raise_for_status()
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
                except Exception:
                    break

                if on_progress:
                    await on_progress(len(records), self.max_pages * 30, f"ingestion:page_{page}")
                await asyncio.sleep(0.4)

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
