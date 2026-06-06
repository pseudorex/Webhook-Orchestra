import httpx
import time
import logging

from datetime import (
    datetime,
    timedelta,
    timezone
)

from app.models.tenant import Tenant

from app.services.signature_service import (
    generate_signature
)

from app.services.reliability.failure_classifier import (
    FailureClassifier
)

from app.services.reliability.retry_policy import (
    RetryPolicy
)

from app.services.reliability.delivery_attempt_service import (
    DeliveryAttemptService
)

from app.services.reliability.dlq_service import (
    DLQService
)

from app.services.reliability.circuit_breaker_service import (
    CircuitBreakerService
)
from app.core.logging import correlation_id_var, tenant_id_var, event_id_var

# ---------------------------------------------------------------------------
# Prometheus metrics — imported once at module level (not inside functions).
# app.core.metrics has no dependency on app.services so there is no circular
# import here. The previous local imports were an unnecessary workaround.
# ---------------------------------------------------------------------------
from app.core.metrics import (
    WEBHOOK_DELIVERY_ATTEMPTS_TOTAL,
    WEBHOOK_DELIVERY_LATENCIES_SECONDS,
    WEBHOOK_RETRIES_TOTAL,
    WEBHOOK_DLQ_MOVES_TOTAL,
)

logger = logging.getLogger(__name__)


class WebhookEngine:

    @staticmethod
    def process_event(db, delivery): # ← Changed parameter to delivery

        start = time.time()

        try:
            from app.models.event import Event
            from app.models.subscription import Subscription

            event = db.get(Event, delivery.event_id)
            if not event:
                raise Exception("EVENT NOT FOUND")

            # Explicitly align context variables for event-linked and tenant-linked logs
            tenant_id_var.set(event.tenant_id)
            event_id_var.set(event.id)

            # FETCH TENANT

            tenant = db.get(
                Tenant,
                event.tenant_id
            )

            if not tenant:
                raise Exception("TENANT NOT FOUND")

            # Increment tenant delivery attempts counter
            tenant.delivery_attempts_count += 1

            # RESOLVE TARGET URL

            target_url = None
            if delivery.subscription_id:
                subscription = db.get(Subscription, delivery.subscription_id)
                if subscription:
                    target_url = subscription.endpoint_url
            if not target_url:
                target_url = tenant.webhook_url

            if not target_url:
                raise Exception("NO ENDPOINT URL AVAILABLE")

            # CIRCUIT BREAKER CHECK
            allowed = CircuitBreakerService.should_allow_request(
                db=db,
                endpoint_url=target_url,
                tenant_id=event.tenant_id,
            )

            if not allowed:
                logger.warning(
                    f"Delivery skipped. Circuit is OPEN for {target_url}",
                    extra={"target_url": target_url}
                )

                # Schedule a retry after cooldown
                WebhookEngine.handle_circuit_open(
                    db=db,
                    delivery=delivery,
                    target_url=target_url,
                )

                return

            # -----------------------------
            # BUILD PAYLOAD
            # -----------------------------

            payload = {
                "event_id": event.id,
                "event_type": event.event_type,
                "payload": event.payload
            }

            # -----------------------------
            # SIGNATURE
            # -----------------------------

            signature = generate_signature(
                payload=payload,
                secret=tenant.webhook_secret
            )

            # -----------------------------
            # SEND WEBHOOK (Propagating Correlation ID outbound)
            # -----------------------------

            headers = {
                "X-Webhook-Signature": signature,
                "X-Event-ID": str(event.id)
            }

            # Fetch current request's correlation ID
            corr_id = correlation_id_var.get()
            if corr_id:
                headers["X-Correlation-ID"] = corr_id

            response = httpx.post(
                target_url,
                json=payload,
                headers=headers,
                timeout=10
            )

            latency = int(
                (time.time() - start) * 1000
            )

            # -----------------------------
            # FAILURE TYPE FOR NON-2XX
            # -----------------------------

            failure_type_value = None

            if response.status_code >= 400:
                failure_type_value = (
                    FailureClassifier.classify(
                        status_code=response.status_code
                    ).value
                )

            # -----------------------------
            # LOG DELIVERY ATTEMPT
            # -----------------------------

            DeliveryAttemptService.log_attempt(
                db=db,
                event_id=event.id,
                attempt_number=delivery.retry_count + 1, # ← Tracked on delivery record
                status_code=response.status_code,
                response_body=response.text,
                response_time_ms=latency,
                failure_type=failure_type_value
            )

            # -----------------------------
            # SUCCESS
            # -----------------------------

            if 200 <= response.status_code < 300:
                delivery.status = "delivered" # ← Tracked on delivery record
                delivery.delivered_at = datetime.now(timezone.utc)
                delivery.last_error = None
                delivery.failure_type = None
                delivery.next_retry_at = None

                # CIRCUIT BREAKER — record success
                CircuitBreakerService.record_success(
                    db=db,
                    endpoint_url=target_url,
                    tenant_id=event.tenant_id,
                    latency_ms=latency,
                )

                db.commit()

                logger.info(
                    "Webhook delivered successfully",
                    extra={"status_code": response.status_code, "endpoint_url": target_url}
                )

                # Prometheus: record delivery success metrics
                latency_sec = latency / 1000.0
                WEBHOOK_DELIVERY_ATTEMPTS_TOTAL.labels(
                    tenant_id=str(event.tenant_id),
                    status_code=str(response.status_code),
                    error_class="None"
                ).inc()
                WEBHOOK_DELIVERY_LATENCIES_SECONDS.labels(tenant_id=str(event.tenant_id)).observe(latency_sec)

                return

            # -----------------------------
            # FAILURE CLASSIFICATION
            # -----------------------------

            failure_type = FailureClassifier.classify(
                status_code=response.status_code
            )

            # CIRCUIT BREAKER — record failure
            CircuitBreakerService.record_failure(
                db=db,
                endpoint_url=target_url,
                tenant_id=event.tenant_id,
                latency_ms=latency,
            )

            # Prometheus: record failed delivery status code metric
            latency_sec = latency / 1000.0
            WEBHOOK_DELIVERY_ATTEMPTS_TOTAL.labels(
                tenant_id=str(event.tenant_id),
                status_code=str(response.status_code),
                error_class=failure_type.value
            ).inc()
            WEBHOOK_DELIVERY_LATENCIES_SECONDS.labels(tenant_id=str(event.tenant_id)).observe(latency_sec)

            WebhookEngine.handle_failure(
                db=db,
                delivery=delivery,
                failure_type=failure_type,
                error_message=response.text,
                target_url=target_url
            )

        except Exception as error:

            latency = int(
                (time.time() - start) * 1000
            )

            # -----------------------------
            # CLASSIFY FAILURE
            # -----------------------------

            failure_type = FailureClassifier.classify(
                error=error
            )

            # -----------------------------
            # LOG ATTEMPT
            # -----------------------------

            DeliveryAttemptService.log_attempt(
                db=db,
                event_id=delivery.event_id,
                attempt_number=delivery.retry_count + 1, # ← Tracked on delivery record
                response_time_ms=latency,
                failure_type=failure_type.value
            )

            # CIRCUIT BREAKER — record failure
            from app.models.event import Event
            event = db.get(Event, delivery.event_id)
            target_url = None
            if delivery.subscription_id:
                from app.models.subscription import Subscription
                subscription = db.get(Subscription, delivery.subscription_id)
                if subscription:
                    target_url = subscription.endpoint_url
            if not target_url and event:
                tenant = db.get(Tenant, event.tenant_id)
                target_url = tenant.webhook_url if tenant else None

            if target_url and event:
                CircuitBreakerService.record_failure(
                    db=db,
                    endpoint_url=target_url,
                    tenant_id=event.tenant_id,
                    latency_ms=latency,
                )

            # Prometheus: record execution exception metric
            if event:
                latency_sec = latency / 1000.0
                WEBHOOK_DELIVERY_ATTEMPTS_TOTAL.labels(
                    tenant_id=str(event.tenant_id),
                    status_code="exception",
                    error_class=failure_type.value
                ).inc()
                WEBHOOK_DELIVERY_LATENCIES_SECONDS.labels(tenant_id=str(event.tenant_id)).observe(latency_sec)

            # -----------------------------
            # HANDLE FAILURE
            # -----------------------------

            WebhookEngine.handle_failure(
                db=db,
                delivery=delivery,
                failure_type=failure_type,
                error_message=str(error),
                target_url=target_url
            )

    # =========================================
    # HANDLE CIRCUIT OPEN — SCHEDULE RETRY
    # =========================================

    @staticmethod
    def handle_circuit_open(
            db,
            delivery,
            target_url
    ):
        from app.models.event import Event
        event = db.get(Event, delivery.event_id)

        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=target_url,
            tenant_id=event.tenant_id,
        )

        delay = circuit.cooldown_seconds

        delivery.status = "circuit_open" # ← Tracked on delivery record
        delivery.next_retry_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=delay)
        )

        # Increment tenant retries counter
        tenant = db.get(Tenant, event.tenant_id)
        if tenant:
            tenant.retries_count += 1

        db.commit()

        logger.info(
            f"Webhook delivery scheduled after circuit cooldown: {delay}s",
            extra={"delay_seconds": delay}
        )

        # Prometheus: track circuit open retry schedule
        WEBHOOK_RETRIES_TOTAL.labels(
            tenant_id=str(event.tenant_id),
            retry_count="circuit_cooldown"
        ).inc()

        # Use celery.send_task() to schedule the retry.
        # This avoids a circular import: webhook_engine <- worker.tasks <- webhook_engine.
        from worker.celery_app import celery

        # Determine target queue based on retry count
        target_queue = "default" if delivery.retry_count <= 2 else "low_priority"

        celery.send_task(
            "worker.tasks.deliver_webhook",
            args=[delivery.id],
            countdown=delay,
            queue=target_queue
        )

    # =========================================
    # HANDLE FAILURE
    # =========================================

    @staticmethod
    def handle_failure(
            db,
            delivery,
            failure_type,
            error_message,
            target_url
    ):
        from app.models.event import Event
        event = db.get(Event, delivery.event_id)

        # -----------------------------
        # UPDATE DELIVERY FAILURE STATE
        # -----------------------------

        delivery.retry_count += 1 # ← Tracked on delivery record
        delivery.failure_type = failure_type.value
        delivery.last_error = error_message

        retryable = FailureClassifier.is_retryable(
            failure_type
        )

        max_retries = RetryPolicy.max_retries(
            failure_type
        )

        # -----------------------------
        # MOVE TO DLQ
        # -----------------------------

        if (
                not retryable
                or delivery.retry_count > max_retries
        ):
            delivery.status = "dead" # ← Tracked on delivery record
            delivery.next_retry_at = None

            DLQService.move_to_dlq(
                db=db,
                event_id=event.id,
                subscription_delivery_id=delivery.id, # ← Pass subscription_delivery_id
                failure_type=failure_type.value,
                final_error=error_message
            )

            db.commit()

            logger.error(
                "Event moved to DLQ",
                extra={"failure_type": failure_type.value, "error": error_message}
            )

            # Prometheus: track DLQ move metric
            WEBHOOK_DLQ_MOVES_TOTAL.labels(
                tenant_id=str(event.tenant_id),
                failure_type=failure_type.value
            ).inc()

            return

        # -----------------------------
        # RETRY DELAY
        # -----------------------------

        delay = RetryPolicy.get_delay(
            failure_type,
            delivery.retry_count # ← Tracked on delivery record
        )

        retry_time = (
                datetime.now(timezone.utc)
                + timedelta(seconds=delay)
        )

        delivery.status = "retrying" # ← Tracked on delivery record
        delivery.next_retry_at = retry_time

        # Increment tenant retries counter
        tenant = db.get(Tenant, event.tenant_id)
        if tenant:
            tenant.retries_count += 1

        db.commit()

        logger.info(
            f"Webhook scheduled for retry in {delay} seconds",
            extra={"delay_seconds": delay, "failure_type": failure_type.value, "next_retry_at": retry_time.isoformat()}
        )

        # Prometheus: track standard backoff retry schedule
        WEBHOOK_RETRIES_TOTAL.labels(
            tenant_id=str(event.tenant_id),
            retry_count=str(delivery.retry_count)
        ).inc()

        # ---------------------------------------------------------------------------
        # Break the circular import: webhook_engine <- worker.tasks <- webhook_engine
        # by using celery.send_task() with the task name string instead of importing
        # the deliver_webhook function directly.
        # ---------------------------------------------------------------------------
        from worker.celery_app import celery

        # -----------------------------
        # CHECK CIRCUIT BEFORE RETRY
        # -----------------------------

        from app.services.reliability.circuit_breaker_service import (
            CircuitBreakerService
        )

        if target_url:

            circuit = CircuitBreakerService.get_or_create(
                db=db,
                endpoint_url=target_url,
                tenant_id=event.tenant_id,
            )

            if circuit.state == "open":
                delay = circuit.cooldown_seconds

                delivery.status = "circuit_open" # ← Tracked on delivery record
                delivery.next_retry_at = (
                        datetime.now(timezone.utc)
                        + timedelta(seconds=delay)
                )
                db.commit()

                logger.warning(
                    f"Circuit open during retry setup. Using cooldown delay: {delay}s",
                    extra={"delay_seconds": delay}
                )

        # Determine target queue based on retry count
        target_queue = "default" if delivery.retry_count <= 2 else "low_priority"

        # -----------------------------
        # SCHEDULE RETRY
        # -----------------------------

        celery.send_task(
            "worker.tasks.deliver_webhook",
            args=[delivery.id],
            countdown=delay,
            queue=target_queue
        )