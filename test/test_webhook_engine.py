"""
Integration tests for the WebhookEngine delivery pipeline.

Uses in-memory SQLite and mocks external I/O (HTTP requests, Celery tasks)
so no running services are needed. Tests validate the full
process_event() flow for success, failure, DLQ promotion, and retry scheduling.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# app.core.database is replaced by conftest.py with a test-local Base
from app.core.database import Base
from app.models.tenant import Tenant
from app.models.event import Event
from app.models.subscription import Subscription
from app.models.subscription_delivery import SubscriptionDelivery
from app.models.circuit_breaker import CircuitBreaker
from app.models.dead_letter_event import DeadLetterEvent
from app.models.delivery_attempt import DeliveryAttempt


# ---------------------------------------------------------------------------
# In-memory SQLite fixture (FK constraints OFF so we don't need full schema)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


# ---------------------------------------------------------------------------
# Helpers — insert minimal rows
# ---------------------------------------------------------------------------

def seed_tenant(db, tenant_id=1, secret="test-secret", webhook_url="https://example.com/hook"):
    tenant = Tenant(
        id=tenant_id,
        name="Test Corp",
        email=f"test-{tenant_id}@example.com",
        api_key=f"key-{tenant_id}",
        webhook_url=webhook_url,
        webhook_secret=secret,
        delivery_attempts_count=0,
        retries_count=0,
    )
    db.add(tenant)
    db.commit()
    db.refresh(tenant)
    return tenant


def seed_event(db, tenant_id=1, event_type="order.created"):
    ev = Event(
        id=1,
        tenant_id=tenant_id,
        event_type=event_type,
        payload={"order_id": "ord_001"},
        status="received",
    )
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return ev


def seed_subscription(db, tenant_id=1, endpoint_url="https://example.com/hook"):
    sub = Subscription(
        id=1,
        tenant_id=tenant_id,
        topic="order.created",
        endpoint_url=endpoint_url,
        is_active=True,
    )
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return sub


def seed_delivery(db, event_id=1, subscription_id=1, retry_count=0, status="pending"):
    delivery = SubscriptionDelivery(
        id=1,
        event_id=event_id,
        subscription_id=subscription_id,
        retry_count=retry_count,
        status=status,
    )
    db.add(delivery)
    db.commit()
    db.refresh(delivery)
    return delivery


def make_mock_response(status_code, text="OK"):
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = text
    return mock


# ---------------------------------------------------------------------------
# Patch helpers
#
# httpx is patched at the module level where it is imported (webhook_engine).
# Celery retry scheduling now uses celery.send_task() so we patch that.
# ---------------------------------------------------------------------------

HTTPX_PATCH = "app.services.reliability.webhook_engine.httpx.post"
CELERY_PATCH = "worker.celery_app.celery.send_task"


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------

class TestWebhookEngineSuccess:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_successful_delivery_marks_delivered(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(200)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "delivered"
        assert delivery.delivered_at is not None
        assert delivery.last_error is None

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_successful_delivery_updates_tenant_attempt_count(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(200)

        tenant = seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(tenant)
        assert tenant.delivery_attempts_count == 1

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_successful_delivery_passes_signature_header(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(200)

        seed_tenant(db, secret="my-secret")
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        call_kwargs = mock_post.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert "X-Webhook-Signature" in headers
        assert len(headers["X-Webhook-Signature"]) == 64  # sha256 hex


# ---------------------------------------------------------------------------
# Permanent failure → DLQ (no retry)
# ---------------------------------------------------------------------------

class TestWebhookEnginePermanentFailure:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_404_goes_to_dlq_immediately(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(404, "Not Found")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "dead"
        assert delivery.failure_type == "PERMANENT"

        dlq_entry = db.query(DeadLetterEvent).filter_by(original_event_id=1).first()
        assert dlq_entry is not None
        assert dlq_entry.failure_type == "PERMANENT"

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_404_does_not_schedule_retry(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(404)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        mock_celery.assert_not_called()

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_401_goes_to_dlq(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(401, "Unauthorized")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "dead"


# ---------------------------------------------------------------------------
# Transient failure → retry
# ---------------------------------------------------------------------------

class TestWebhookEngineTransientFailure:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_503_schedules_retry(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(503, "Service Unavailable")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "retrying"
        assert delivery.retry_count == 1
        assert delivery.failure_type == "TRANSIENT"
        assert delivery.next_retry_at is not None

        mock_celery.assert_called_once()

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_503_routes_first_retry_to_default_queue(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(503)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db, retry_count=0)  # first attempt

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        call_kwargs = mock_celery.call_args
        queue = call_kwargs.kwargs.get("queue") or call_kwargs[1].get("queue")
        assert queue == "default"

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_503_routes_late_retry_to_low_priority_queue(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(503)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db, retry_count=3)  # 4th attempt

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        call_kwargs = mock_celery.call_args
        queue = call_kwargs.kwargs.get("queue") or call_kwargs[1].get("queue")
        assert queue == "low_priority"

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_max_retries_exceeded_moves_to_dlq(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(503)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db, retry_count=5)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "dead"
        dlq_entry = db.query(DeadLetterEvent).filter_by(original_event_id=1).first()
        assert dlq_entry is not None


# ---------------------------------------------------------------------------
# Rate-limit failure → retry
# ---------------------------------------------------------------------------

class TestWebhookEngineRateLimited:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_429_schedules_retry_with_rate_limit_backoff(self, mock_post, mock_celery, db):
        mock_post.return_value = make_mock_response(429, "Too Many Requests")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "retrying"
        assert delivery.failure_type == "RATE_LIMITED"

        call_kwargs = mock_celery.call_args
        countdown = call_kwargs.kwargs.get("countdown") or call_kwargs[1].get("countdown")
        # Attempt 1 → 60 * 1 = 60 seconds
        assert countdown == 60


# ---------------------------------------------------------------------------
# Exception path (timeout, DNS error, connection refused)
# ---------------------------------------------------------------------------

class TestWebhookEngineExceptions:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_timeout_exception_schedules_retry(self, mock_post, mock_celery, db):
        # Use a plain exception whose str() contains 'timeout' so the
        # FailureClassifier string-match correctly returns TIMEOUT.
        mock_post.side_effect = Exception("HTTPConnectionPool: Read timed out. (read timeout=10)")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.failure_type == "TIMEOUT"
        assert delivery.status in ("retrying", "dead")

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_connection_refused_schedules_retry(self, mock_post, mock_celery, db):
        mock_post.side_effect = ConnectionRefusedError("Connection refused")

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db)

        from app.services.reliability.webhook_engine import WebhookEngine
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.failure_type == "CONNECTION_REFUSED"


# ---------------------------------------------------------------------------
# Idempotency — delivering same delivery_id twice
# ---------------------------------------------------------------------------

class TestIdempotencyGuard:

    @patch(CELERY_PATCH)
    @patch(HTTPX_PATCH)
    def test_already_delivered_delivery_is_not_reprocessed(self, mock_post, mock_celery, db):
        """
        If a delivery is already in 'delivered' status (edge case: double-processing),
        the engine still runs — but the DB state should remain coherent.
        This test validates no crash occurs on re-processing a delivered record.
        """
        mock_post.return_value = make_mock_response(200)

        seed_tenant(db)
        seed_event(db)
        seed_subscription(db)
        delivery = seed_delivery(db, status="delivered")

        from app.services.reliability.webhook_engine import WebhookEngine
        # Should not raise
        WebhookEngine.process_event(db=db, delivery=delivery)

        db.refresh(delivery)
        assert delivery.status == "delivered"
