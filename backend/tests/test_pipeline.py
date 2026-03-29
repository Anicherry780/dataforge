"""
Unit tests for core pipeline components:
- Generic quality check helpers (completeness, uniqueness, timeliness)
- Deduplication logic
"""
import pytest
from datetime import datetime, timezone, timedelta

from app.pipeline.quality import (
    check_completeness,
    check_uniqueness,
    check_timeliness,
    run_all_checks,
)
from app.pipeline.transformation import deduplicate


def _make_crypto(**overrides):
    base = {
        "coin_id": "bitcoin",
        "symbol": "BTC",
        "name": "Bitcoin",
        "current_price": 65000.0,
        "market_cap": 1200000000000,
        "market_cap_rank": 1,
        "total_volume": 30000000000,
        "high_24h": 66000.0,
        "low_24h": 64000.0,
        "price_change_pct_1h": 0.5,
        "price_change_pct_24h": 2.1,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    return {**base, **overrides}


# ── Completeness ──────────────────────────────────────────────────────────────

class TestCompletenessCheck:
    def test_all_complete(self):
        records = [_make_crypto() for _ in range(100)]
        result = check_completeness(records, ["coin_id", "current_price", "market_cap", "symbol"])
        assert result["pass_rate"] == 100.0
        assert result["records_failed"] == 0
        assert result["status"] == "pass"

    def test_null_field(self):
        records = [_make_crypto(current_price=None)]
        result = check_completeness(records, ["coin_id", "current_price"])
        assert result["records_failed"] == 1
        assert result["pass_rate"] == 0.0

    def test_empty_string_field(self):
        records = [_make_crypto(coin_id=""), _make_crypto()]
        result = check_completeness(records, ["coin_id"])
        assert result["records_failed"] == 1


# ── Uniqueness ────────────────────────────────────────────────────────────────

class TestUniquenessCheck:
    def test_no_duplicates(self):
        records = [_make_crypto(coin_id=f"coin-{i}") for i in range(100)]
        result = check_uniqueness(records, "coin_id")
        assert result["records_failed"] == 0
        assert result["pass_rate"] == 100.0

    def test_with_duplicates(self):
        records = [_make_crypto(coin_id="bitcoin") for _ in range(5)]
        result = check_uniqueness(records, "coin_id")
        assert result["records_failed"] == 4


# ── Timeliness ────────────────────────────────────────────────────────────────

class TestTimelinessCheck:
    def test_valid_timestamp(self):
        records = [_make_crypto()]
        result = check_timeliness(records, "last_updated")
        assert result["records_failed"] == 0

    def test_future_timestamp(self):
        future = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        records = [_make_crypto(last_updated=future)]
        result = check_timeliness(records, "last_updated")
        assert result["records_failed"] == 1

    def test_too_old_timestamp(self):
        old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        records = [_make_crypto(last_updated=old)]
        result = check_timeliness(records, "last_updated")
        assert result["records_failed"] == 1


# ── Run All Checks (crypto) ──────────────────────────────────────────────────

class TestRunAllChecks:
    def test_composite_score(self):
        records = [_make_crypto(coin_id=f"coin-{i}") for i in range(50)]
        checks, score = run_all_checks(records, source_type="crypto")
        assert len(checks) == 5
        assert 0 <= score <= 100
        assert score > 95


# ── Deduplication ─────────────────────────────────────────────────────────────

class TestDeduplicate:
    def test_removes_dupes(self):
        records = [_make_crypto(coin_id="bitcoin") for _ in range(5)]
        unique, removed = deduplicate(records, "coin_id")
        assert len(unique) == 1
        assert removed == 4

    def test_no_dupes(self):
        records = [_make_crypto(coin_id=f"coin-{i}") for i in range(10)]
        unique, removed = deduplicate(records, "coin_id")
        assert len(unique) == 10
        assert removed == 0
