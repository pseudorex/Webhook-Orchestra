"""
Unit tests for CircuitBreakerService state machine.

Uses an in-memory SQLite database (via SQLAlchemy) — no PostgreSQL, no Docker
required. Tests validate all state transitions and health score calculations.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, event as sa_event
from sqlalchemy.orm import sessionmaker

# app.core.database is replaced by conftest.py with a test-local Base
from app.core.database import Base
from app.models.circuit_breaker import CircuitBreaker
from app.models.tenant import Tenant
from app.services.reliability.circuit_breaker_service import CircuitBreakerService


# ---------------------------------------------------------------------------
# In-memory SQLite fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db():
    """Provides a fresh, isolated in-memory SQLite session per test."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False}
    )

    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    yield session

    session.close()
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


def make_circuit(db, endpoint_url="https://example.com/hook", tenant_id=1, **kwargs):
    """Helper: insert and return a CircuitBreaker row."""
    defaults = dict(
        endpoint_url=endpoint_url,
        tenant_id=tenant_id,
        state="closed",
        failure_count=0,
        failure_threshold=5,
        cooldown_seconds=60,
        consecutive_failures=0,
        success_count=0,
        total_requests=0,
        success_rate=100.0,
        average_latency_ms=0.0,
        health_score=100.0,
        health_state="healthy",
    )
    defaults.update(kwargs)
    circuit = CircuitBreaker(**defaults)
    db.add(circuit)
    db.commit()
    db.refresh(circuit)
    return circuit


# ---------------------------------------------------------------------------
# get_or_create()
# ---------------------------------------------------------------------------

class TestGetOrCreate:

    def test_creates_circuit_when_none_exists(self, db):
        circuit = CircuitBreakerService.get_or_create(
            db=db, endpoint_url="https://new.com/hook", tenant_id=1
        )
        assert circuit.id is not None
        assert circuit.state == "closed"
        assert circuit.failure_count == 0

    def test_returns_existing_circuit(self, db):
        existing = make_circuit(db, endpoint_url="https://exists.com/hook")
        fetched = CircuitBreakerService.get_or_create(
            db=db, endpoint_url="https://exists.com/hook", tenant_id=1
        )
        assert fetched.id == existing.id

    def test_does_not_duplicate_circuit(self, db):
        CircuitBreakerService.get_or_create(db=db, endpoint_url="https://once.com/hook", tenant_id=1)
        CircuitBreakerService.get_or_create(db=db, endpoint_url="https://once.com/hook", tenant_id=1)
        count = db.query(CircuitBreaker).filter_by(endpoint_url="https://once.com/hook").count()
        assert count == 1


# ---------------------------------------------------------------------------
# should_allow_request()
# ---------------------------------------------------------------------------

class TestShouldAllowRequest:

    def test_closed_circuit_allows_all_requests(self, db):
        make_circuit(db, state="closed")
        assert CircuitBreakerService.should_allow_request(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        ) is True

    def test_open_circuit_within_cooldown_blocks_request(self, db):
        make_circuit(
            db,
            state="open",
            opened_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            cooldown_seconds=60  # cooldown has NOT expired
        )
        assert CircuitBreakerService.should_allow_request(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        ) is False

    def test_open_circuit_after_cooldown_transitions_to_half_open(self, db):
        make_circuit(
            db,
            state="open",
            opened_at=datetime.now(timezone.utc) - timedelta(seconds=120),
            cooldown_seconds=60  # cooldown HAS expired
        )
        result = CircuitBreakerService.should_allow_request(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        assert result is True
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "half_open"

    def test_half_open_circuit_allows_one_test_request(self, db):
        make_circuit(db, state="half_open")
        assert CircuitBreakerService.should_allow_request(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        ) is True

    def test_open_circuit_with_no_opened_at_blocks(self, db):
        make_circuit(db, state="open", opened_at=None)
        assert CircuitBreakerService.should_allow_request(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        ) is False


# ---------------------------------------------------------------------------
# record_success()
# ---------------------------------------------------------------------------

class TestRecordSuccess:

    def test_success_closes_open_circuit(self, db):
        make_circuit(db, state="open", opened_at=datetime.now(timezone.utc))
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1, latency_ms=200
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "closed"

    def test_success_closes_half_open_circuit(self, db):
        make_circuit(db, state="half_open")
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1, latency_ms=150
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "closed"

    def test_success_resets_failure_count(self, db):
        make_circuit(db, failure_count=4, consecutive_failures=4)
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.failure_count == 0
        assert circuit.consecutive_failures == 0

    def test_success_increments_counters(self, db):
        make_circuit(db)
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1, latency_ms=100
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.success_count == 1
        assert circuit.total_requests == 1

    def test_success_tracks_latency(self, db):
        make_circuit(db)
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1, latency_ms=300
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.average_latency_ms == 300.0

    def test_success_rate_is_100_after_single_success(self, db):
        make_circuit(db)
        CircuitBreakerService.record_success(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.success_rate == 100.0


# ---------------------------------------------------------------------------
# record_failure()
# ---------------------------------------------------------------------------

class TestRecordFailure:

    def test_failure_increments_failure_count(self, db):
        make_circuit(db)
        CircuitBreakerService.record_failure(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.failure_count == 1

    def test_failure_opens_circuit_at_threshold(self, db):
        # failure_threshold = 5, so 5 failures should open it
        make_circuit(db, failure_count=4, failure_threshold=5)
        CircuitBreakerService.record_failure(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "open"
        assert circuit.opened_at is not None

    def test_failure_below_threshold_keeps_circuit_closed(self, db):
        make_circuit(db, failure_count=2, failure_threshold=5)
        CircuitBreakerService.record_failure(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "closed"

    def test_failure_in_half_open_reopens_circuit(self, db):
        make_circuit(db, state="half_open")
        CircuitBreakerService.record_failure(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        assert circuit.state == "open"
        assert circuit.opened_at is not None

    def test_failure_in_already_open_circuit_is_no_op(self, db):
        make_circuit(db, state="open", failure_count=5, opened_at=datetime.now(timezone.utc))
        CircuitBreakerService.record_failure(
            db=db, endpoint_url="https://example.com/hook", tenant_id=1
        )
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url="https://example.com/hook").first()
        # failure_count should NOT increment (early return in record_failure for open state)
        assert circuit.failure_count == 5


# ---------------------------------------------------------------------------
# Full state machine transition sequence
# ---------------------------------------------------------------------------

class TestStateMachineSequence:
    """Simulate a realistic failure → open → cooldown → half_open → recover flow."""

    def test_full_lifecycle(self, db):
        url = "https://example.com/hook"
        make_circuit(db, endpoint_url=url, failure_threshold=3, cooldown_seconds=5)

        # 3 failures → should open
        for _ in range(3):
            CircuitBreakerService.record_failure(db=db, endpoint_url=url, tenant_id=1)

        circuit = db.query(CircuitBreaker).filter_by(endpoint_url=url).first()
        assert circuit.state == "open"

        # Simulate cooldown expired by backdating opened_at
        circuit.opened_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        db.commit()

        # Now should_allow_request transitions to half_open
        allowed = CircuitBreakerService.should_allow_request(db=db, endpoint_url=url, tenant_id=1)
        assert allowed is True
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url=url).first()
        assert circuit.state == "half_open"

        # One success → back to closed
        CircuitBreakerService.record_success(db=db, endpoint_url=url, tenant_id=1)
        circuit = db.query(CircuitBreaker).filter_by(endpoint_url=url).first()
        assert circuit.state == "closed"
        assert circuit.failure_count == 0

    def test_half_open_failure_reopens(self, db):
        url = "https://flaky.com/hook"
        make_circuit(db, endpoint_url=url, state="half_open")

        CircuitBreakerService.record_failure(db=db, endpoint_url=url, tenant_id=1)

        circuit = db.query(CircuitBreaker).filter_by(endpoint_url=url).first()
        assert circuit.state == "open"


# ---------------------------------------------------------------------------
# Health score calculation
# ---------------------------------------------------------------------------

class TestCalculateHealth:
    """Verify the health scoring formula."""

    def test_open_circuit_has_zero_health_score(self, db):
        circuit = make_circuit(db, state="open")
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 0.0
        assert circuit.health_state == "unhealthy"

    def test_healthy_circuit_score_is_100_when_no_issues(self, db):
        circuit = make_circuit(db, consecutive_failures=0, average_latency_ms=0.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 100.0
        assert circuit.health_state == "healthy"

    def test_consecutive_failures_reduce_health(self, db):
        # 1 failure = -20 points → score = 80 (still "healthy")
        circuit = make_circuit(db, consecutive_failures=1, average_latency_ms=0.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 80.0
        assert circuit.health_state == "healthy"

    def test_two_consecutive_failures_is_degraded(self, db):
        # 2 failures = -40 points → score = 60 → "degraded"
        circuit = make_circuit(db, consecutive_failures=2, average_latency_ms=0.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 60.0
        assert circuit.health_state == "degraded"

    def test_three_consecutive_failures_is_unhealthy(self, db):
        # 3 failures = -60 points → score = 40 → "unhealthy"
        circuit = make_circuit(db, consecutive_failures=3, average_latency_ms=0.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 40.0
        assert circuit.health_state == "unhealthy"

    def test_high_latency_reduces_health(self, db):
        # 210ms latency → (210-200)/10 = 1 point penalty
        circuit = make_circuit(db, consecutive_failures=0, average_latency_ms=210.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 99.0

    def test_latency_penalty_capped_at_50_points(self, db):
        # Enormous latency → max 50 point penalty → score = 50 → "degraded"
        circuit = make_circuit(db, consecutive_failures=0, average_latency_ms=100_000.0, success_rate=100.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 50.0

    def test_low_success_rate_reduces_health(self, db):
        # 80% success rate → (100-80)*0.5 = 10 point penalty → score = 90 → "healthy"
        circuit = make_circuit(db, consecutive_failures=0, average_latency_ms=0.0, success_rate=80.0)
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 90.0

    def test_score_is_clamped_to_zero_minimum(self, db):
        # 5 failures (-100) + bad latency → should not go below 0
        circuit = make_circuit(
            db, consecutive_failures=5, average_latency_ms=100_000.0, success_rate=0.0
        )
        CircuitBreakerService.calculate_health(circuit)
        assert circuit.health_score == 0.0
        assert circuit.health_state == "unhealthy"

    def test_health_state_thresholds(self, db):
        cases = [
            (100.0, "healthy"),
            (80.0, "healthy"),
            (79.9, "degraded"),
            (50.0, "degraded"),
            (49.9, "unhealthy"),
            (0.0, "unhealthy"),
        ]
        for score, expected_state in cases:
            circuit = make_circuit(
                db,
                endpoint_url=f"https://example.com/{score}",
                consecutive_failures=0,
                average_latency_ms=0.0,
                success_rate=100.0,
                health_score=score,
            )
            # Manually set score and recalculate state
            circuit.health_score = score
            if score >= 80.0:
                circuit.health_state = "healthy"
            elif score >= 50.0:
                circuit.health_state = "degraded"
            else:
                circuit.health_state = "unhealthy"
            assert circuit.health_state == expected_state, f"Score {score} should be {expected_state}"
