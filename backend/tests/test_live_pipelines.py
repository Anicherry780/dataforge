"""
Tests for live pipeline logic: crypto, weather, and GitHub.
Covers quality checks (completeness, validity, uniqueness, consistency)
and transformation/enrichment functions.
"""
import pytest
from datetime import datetime, timezone

from app.pipeline.quality import (
    run_all_checks,
    check_completeness,
    check_validity,
    check_uniqueness,
    check_consistency,
)
from app.pipeline.transformation import (
    _enrich_crypto,
    _enrich_weather,
    _enrich_github,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_crypto(overrides: dict | None = None) -> dict:
    base = {
        "coin_id": "bitcoin",
        "symbol": "btc",
        "current_price": 60000,
        "market_cap": 1_200_000_000_000,
        "market_cap_rank": 1,
        "total_volume": 30_000_000_000,
        "high_24h": 61000,
        "low_24h": 59000,
        "price_change_pct_1h": 0.5,
        "price_change_pct_24h": 1.2,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    if overrides:
        base.update(overrides)
    return base


def _make_weather(overrides: dict | None = None) -> dict:
    base = {
        "city": "Austin",
        "temperature_c": 30,
        "humidity_pct": 55,
        "apparent_temp_c": 32,
        "wind_speed_kmh": 15,
        "precipitation_mm": 0,
        "condition": "Sunny",
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    if overrides:
        base.update(overrides)
    return base


def _make_github(overrides: dict | None = None) -> dict:
    base = {
        "event_id": "evt_001",
        "event_type": "PushEvent",
        "actor": "octocat",
        "repo_name": "octocat/Hello-World",
        "is_public": True,
        "push_size": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if overrides:
        base.update(overrides)
    return base


# ── Crypto Quality Checks ────────────────────────────────────────────────────

class TestCryptoQuality:
    def test_completeness_all_fields_present(self):
        records = [_make_crypto(), _make_crypto({"coin_id": "ethereum", "symbol": "eth"})]
        result = check_completeness(records, ["coin_id", "current_price", "market_cap", "symbol"])
        assert result["pass_rate"] == 100.0
        assert result["records_failed"] == 0

    def test_validity_negative_price(self):
        records = [_make_crypto({"current_price": -100})]
        result = check_validity(records, source_type="crypto")
        assert result["records_failed"] == 1
        assert "invalid_price" in result["details"]

    def test_validity_negative_market_cap(self):
        records = [_make_crypto({"market_cap": -5000})]
        result = check_validity(records, source_type="crypto")
        assert result["records_failed"] == 1
        assert "negative_market_cap" in result["details"]

    def test_consistency_high_lt_low(self):
        records = [_make_crypto({"high_24h": 50000, "low_24h": 55000})]
        result = check_consistency(records, source_type="crypto")
        assert result["records_failed"] == 1
        assert "high_lt_low" in result["details"]

    def test_uniqueness_duplicate_coin_ids(self):
        records = [_make_crypto(), _make_crypto()]  # same coin_id "bitcoin"
        result = check_uniqueness(records, key_field="coin_id")
        assert result["records_failed"] == 1
        assert "duplicate" in result["details"].lower()


# ── Weather Quality Checks ────────────────────────────────────────────────────

class TestWeatherQuality:
    def test_validity_impossible_temperature(self):
        records = [_make_weather({"temperature_c": 70})]
        result = check_validity(records, source_type="weather")
        assert result["records_failed"] == 1
        assert "impossible_temperature" in result["details"]

    def test_validity_humidity_over_100(self):
        records = [_make_weather({"humidity_pct": 120})]
        result = check_validity(records, source_type="weather")
        assert result["records_failed"] == 1
        assert "invalid_humidity" in result["details"]

    def test_validity_negative_wind_speed(self):
        records = [_make_weather({"wind_speed_kmh": -10})]
        result = check_validity(records, source_type="weather")
        assert result["records_failed"] == 1
        assert "negative_wind" in result["details"]

    def test_consistency_huge_temp_apparent_temp_gap(self):
        records = [_make_weather({"temperature_c": 30, "apparent_temp_c": 60})]
        result = check_consistency(records, source_type="weather")
        assert result["records_failed"] == 1
        assert "implausible" in result["details"].lower()


# ── GitHub Quality Checks ─────────────────────────────────────────────────────

class TestGitHubQuality:
    def test_validity_unknown_event_type(self):
        records = [_make_github({"event_type": "TotallyMadeUpEvent"})]
        result = check_validity(records, source_type="github")
        assert result["records_failed"] == 1
        assert "unknown_event_type" in result["details"]

    def test_consistency_non_public_events(self):
        records = [_make_github({"is_public": False})]
        result = check_consistency(records, source_type="github")
        assert result["records_failed"] == 1
        assert "non-public" in result["details"].lower()


# ── Crypto Transformation ─────────────────────────────────────────────────────

class TestCryptoTransformation:
    def test_volatility_pct_calculation(self):
        rec = _make_crypto({"current_price": 100, "high_24h": 110, "low_24h": 90})
        enriched, _ = _enrich_crypto([rec])
        # volatility = (110 - 90) / 100 * 100 = 20.0
        assert enriched[0]["volatility_pct"] == 20.0

    def test_anomaly_flagged_by_zscore(self):
        """One extreme outlier among many similar prices triggers z-score anomaly."""
        normal = [_make_crypto({"coin_id": f"coin_{i}", "current_price": 100}) for i in range(20)]
        outlier = _make_crypto({"coin_id": "outlier", "current_price": 100_000})
        enriched, anomaly_count = _enrich_crypto(normal + [outlier])
        outlier_rec = [r for r in enriched if r["coin_id"] == "outlier"][0]
        assert outlier_rec["is_anomaly"] is True
        assert "z-score" in outlier_rec["anomaly_reason"]
        assert anomaly_count >= 1

    def test_signal_bullish(self):
        # momentum = 0.6 * 5 + 0.4 * 3 = 4.2 > 2 => bullish
        rec = _make_crypto({"price_change_pct_1h": 5, "price_change_pct_24h": 3})
        enriched, _ = _enrich_crypto([rec])
        assert enriched[0]["signal"] == "bullish"

    def test_signal_bearish(self):
        # momentum = 0.6 * (-5) + 0.4 * (-3) = -4.2 < -2 => bearish
        rec = _make_crypto({"price_change_pct_1h": -5, "price_change_pct_24h": -3})
        enriched, _ = _enrich_crypto([rec])
        assert enriched[0]["signal"] == "bearish"

    def test_signal_neutral(self):
        # momentum = 0.6 * 0 + 0.4 * 0 = 0 => neutral
        rec = _make_crypto({"price_change_pct_1h": 0, "price_change_pct_24h": 0})
        enriched, _ = _enrich_crypto([rec])
        assert enriched[0]["signal"] == "neutral"


# ── Weather Transformation ────────────────────────────────────────────────────

class TestWeatherTransformation:
    def test_heat_index_calculated_when_hot_and_humid(self):
        rec = _make_weather({"temperature_c": 35, "humidity_pct": 60})
        enriched, _ = _enrich_weather([rec])
        hi = enriched[0]["heat_index_c"]
        # heat_index should differ from raw temp when formula kicks in
        assert hi != 35
        assert isinstance(hi, float)

    def test_heat_index_equals_temp_when_cool(self):
        rec = _make_weather({"temperature_c": 20, "humidity_pct": 30})
        enriched, _ = _enrich_weather([rec])
        assert enriched[0]["heat_index_c"] == 20

    def test_extreme_flag_when_temp_above_40(self):
        rec = _make_weather({"temperature_c": 45})
        enriched, _ = _enrich_weather([rec])
        assert enriched[0]["is_extreme"] is True
        assert enriched[0]["is_anomaly"] is True


# ── GitHub Transformation ─────────────────────────────────────────────────────

class TestGitHubTransformation:
    def test_bot_actor_flagged_as_anomaly(self):
        rec = _make_github({"actor": "dependabot[bot]"})
        enriched, anomaly_count = _enrich_github([rec])
        assert enriched[0]["is_anomaly"] is True
        assert "bot actor" in enriched[0]["anomaly_reason"]
        assert anomaly_count == 1

    def test_trending_repo_detected(self):
        """A repo with 3+ events in the batch is flagged as trending."""
        records = [
            _make_github({"event_id": f"evt_{i}", "repo_name": "hot/repo"})
            for i in range(4)
        ]
        enriched, _ = _enrich_github(records)
        assert all(r["is_trending_repo"] for r in enriched)
        assert enriched[0]["repo_event_count"] == 4

    def test_non_trending_repo(self):
        records = [
            _make_github({"event_id": "evt_1", "repo_name": "quiet/repo"}),
            _make_github({"event_id": "evt_2", "repo_name": "quiet/repo"}),
        ]
        enriched, _ = _enrich_github(records)
        assert not enriched[0]["is_trending_repo"]
