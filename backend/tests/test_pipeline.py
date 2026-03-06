"""
Unit tests for pipeline components:
- Data ingestion generators
- Quality checks
- Transformation logic
"""
import asyncio
import pytest
from datetime import datetime, timezone, timedelta


# ── Quality check tests ────────────────────────────────────────────────────────

from app.pipeline.quality import (
    check_completeness,
    check_validity,
    check_uniqueness,
    check_timeliness,
    check_consistency,
    run_all_checks,
)


def make_record(**overrides):
    base = {
        "order_id": "ORD-TEST001",
        "user_id": "USR-0001",
        "product_id": "PROD-0001",
        "category": "Electronics",
        "amount": 99.99,
        "quantity": 2,
        "country": "US",
        "status": "completed",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return {**base, **overrides}


class TestCompletenessCheck:
    def test_all_complete(self):
        records = [make_record() for _ in range(100)]
        result = check_completeness(records)
        assert result["pass_rate"] == 100.0
        assert result["records_failed"] == 0
        assert result["status"] == "pass"

    def test_null_amount(self):
        records = [make_record(amount=None)]
        result = check_completeness(records)
        assert result["records_failed"] == 1
        assert result["pass_rate"] == 0.0

    def test_missing_order_id(self):
        records = [make_record(order_id=""), make_record()]
        result = check_completeness(records)
        assert result["records_failed"] == 1


class TestValidityCheck:
    def test_all_valid(self):
        records = [make_record() for _ in range(50)]
        result = check_validity(records)
        assert result["pass_rate"] == 100.0

    def test_negative_amount(self):
        records = [make_record(amount=-10.0)]
        result = check_validity(records)
        assert result["records_failed"] == 1

    def test_invalid_country(self):
        records = [make_record(country="XX")]
        result = check_validity(records)
        assert result["records_failed"] == 1

    def test_invalid_status(self):
        records = [make_record(status="unknown_status")]
        result = check_validity(records)
        assert result["records_failed"] == 1


class TestUniquenessCheck:
    def test_no_duplicates(self):
        records = [make_record(order_id=f"ORD-{i:04d}") for i in range(100)]
        result = check_uniqueness(records)
        assert result["records_failed"] == 0
        assert result["pass_rate"] == 100.0

    def test_with_duplicates(self):
        records = [make_record(order_id="ORD-DUPE") for _ in range(5)]
        result = check_uniqueness(records)
        assert result["records_failed"] == 4  # first is OK, 4 are dupes


class TestTimelinessCheck:
    def test_valid_timestamp(self):
        records = [make_record()]
        result = check_timeliness(records)
        assert result["records_failed"] == 0

    def test_future_timestamp(self):
        future = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        records = [make_record(created_at=future)]
        result = check_timeliness(records)
        assert result["records_failed"] == 1

    def test_too_old_timestamp(self):
        old = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        records = [make_record(created_at=old)]
        result = check_timeliness(records)
        assert result["records_failed"] == 1


class TestConsistencyCheck:
    def test_consistent(self):
        records = [make_record()]
        result = check_consistency(records)
        assert result["records_failed"] == 0

    def test_zero_amount_completed(self):
        records = [make_record(amount=0.0, status="completed")]
        result = check_consistency(records)
        assert result["records_failed"] == 1


class TestRunAllChecks:
    def test_composite_score(self):
        records = [make_record(order_id=f"ORD-{i:04d}") for i in range(200)]
        checks, score = run_all_checks(records)
        assert len(checks) == 5
        assert 0 <= score <= 100
        # All records are valid so score should be high
        assert score > 95


# ── Transformation tests ───────────────────────────────────────────────────────

from app.pipeline.transformation import (
    deduplicate,
    cast_types,
    filter_invalid,
    enrich,
    detect_anomalies,
    compute_aggregations,
)


class TestDeduplicate:
    def test_removes_dupes(self):
        records = [make_record(order_id="ORD-X") for _ in range(5)]
        unique, removed = deduplicate(records)
        assert len(unique) == 1
        assert removed == 4

    def test_no_dupes(self):
        records = [make_record(order_id=f"ORD-{i}") for i in range(10)]
        unique, removed = deduplicate(records)
        assert len(unique) == 10
        assert removed == 0


class TestCastTypes:
    def test_string_amount_cast(self):
        records = [make_record(amount="123.45")]
        result = cast_types(records)
        assert isinstance(result[0]["amount"], float)
        assert result[0]["amount"] == 123.45

    def test_none_amount_defaults(self):
        records = [make_record(amount=None)]
        result = cast_types(records)
        assert result[0]["amount"] == 0.0


class TestFilterInvalid:
    def test_removes_incomplete(self):
        records = [
            make_record(),  # valid
            {"amount": 10.0},  # missing required fields
            make_record(order_id=""),  # empty order_id
        ]
        valid, dropped = filter_invalid(records)
        assert len(valid) == 1
        assert dropped == 2


class TestEnrich:
    def test_adds_revenue(self):
        records = [make_record(amount=50.0, quantity=2)]
        result = enrich(records)
        assert "revenue" in result[0]
        assert "unit_price" in result[0]
        assert result[0]["unit_price"] == 25.0

    def test_adds_temporal_features(self):
        records = [make_record()]
        result = enrich(records)
        assert "day_of_week" in result[0]
        assert "hour_of_day" in result[0]
        assert result[0]["day_of_week"] in [
            "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"
        ]


class TestComputeAggregations:
    def test_basic_aggregation(self):
        records = [
            {**make_record(order_id=f"ORD-{i}", amount=100.0, revenue=100.0, category="Electronics"), "is_anomaly": False}
            for i in range(10)
        ]
        agg = compute_aggregations(records)
        assert agg["total_records"] == 10
        assert agg["total_revenue"] == 1000.0
        assert agg["avg_order_value"] == 100.0


# ── Ingestion tests ────────────────────────────────────────────────────────────

from app.pipeline.ingestion import CsvIngestion, ApiIngestion


@pytest.mark.asyncio
class TestIngestion:
    async def test_csv_ingestion_returns_correct_count(self):
        source = CsvIngestion(num_records=100)
        records = await source.fetch()
        assert len(records) == 100

    async def test_api_ingestion_returns_records(self):
        source = ApiIngestion(num_records=200, page_size=50)
        records = await source.fetch()
        assert len(records) == 200
        assert all("order_id" in r for r in records)
        assert all("source" in r and r["source"] == "api" for r in records)

    async def test_records_have_required_fields(self):
        source = CsvIngestion(num_records=50)
        records = await source.fetch()
        required = {"order_id", "user_id", "product_id", "amount", "quantity", "country", "status"}
        for r in records:
            assert required.issubset(set(r.keys())), f"Missing fields in record: {r}"
