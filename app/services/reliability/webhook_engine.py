import requests
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

logger = logging.getLogger(__name__)


class WebhookEngine:

    @staticmethod
    def process_event(db, event, endpoint_url=None, subscription_id=None):

        start = time.time()

        try:
            # Explicitly align context variables for event-linked and tenant-linked logs
            tenant_id_var.set(event.tenant_id)
            event_id_var.set(event.id)

            # -----------------------------
            # FETCH TENANT
            # -----------------------------

            tenant = db.get(
                Tenant,
                event.tenant_id
            )

            if not tenant:
                raise Exception("TENANT NOT FOUND")

            # -----------------------------
            # RESOLVE TARGET URL
            # -----------------------------

            target_url = endpoint_url or tenant.webhook_url

            if not target_url:
                raise Exception("NO ENDPOINT URL AVAILABLE")

            # =============================
            # CIRCUIT BREAKER CHECK
            # =============================

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
                    event=event,
                    endpoint_url=endpoint_url,
                    subscription_id=subscription_id,
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

            response = requests.post(
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
                attempt_number=event.retry_count + 1,
                status_code=response.status_code,
                response_body=response.text,
                response_time_ms=latency,
                failure_type=failure_type_value
            )

            # -----------------------------
            # SUCCESS
            # -----------------------------

            if 200 <= response.status_code < 300:
                event.status = "delivered"
                event.delivered_at = datetime.now(timezone.utc)
                event.last_error = None
                event.failure_type = None
                event.retryable = True
                event.next_retry_at = None

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

            WebhookEngine.handle_failure(
                db=db,
                event=event,
                failure_type=failure_type,
                error_message=response.text,
                endpoint_url=endpoint_url,
                subscription_id=subscription_id
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
                event_id=event.id,
                attempt_number=event.retry_count + 1,
                response_time_ms=latency,
                failure_type=failure_type.value
            )

            # CIRCUIT BREAKER — record failure
            target_url = endpoint_url or getattr(
                db.get(Tenant, event.tenant_id),
                'webhook_url',
                None
            )

            if target_url:
                CircuitBreakerService.record_failure(
                    db=db,
                    endpoint_url=target_url,
                    tenant_id=event.tenant_id,
                    latency_ms=latency,
                )

            # -----------------------------
            # HANDLE FAILURE
            # -----------------------------

            WebhookEngine.handle_failure(
                db=db,
                event=event,
                failure_type=failure_type,
                error_message=str(error),
                endpoint_url=endpoint_url,
                subscription_id=subscription_id
            )

    # =========================================
    # HANDLE CIRCUIT OPEN — SCHEDULE RETRY
    # =========================================

    @staticmethod
    def handle_circuit_open(
            db,
            event,
            endpoint_url,
            subscription_id,
            target_url
    ):

        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=target_url,
            tenant_id=event.tenant_id,
        )

        delay = circuit.cooldown_seconds

        event.status = "circuit_open"
        event.next_retry_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=delay)
        )

        db.commit()

        logger.info(
            f"Webhook delivery scheduled after circuit cooldown: {delay}s",
            extra={"delay_seconds": delay}
        )

        from worker.tasks import deliver_webhook

        # Determine target queue based on retry count
        target_queue = "default" if event.retry_count <= 2 else "low_priority"

        deliver_webhook.apply_async(
            args=[event.id, endpoint_url, subscription_id],
            countdown=delay,
            queue=target_queue
        )

    # =========================================
    # HANDLE FAILURE
    # =========================================

    @staticmethod
    def handle_failure(
            db,
            event,
            failure_type,
            error_message,
            endpoint_url=None,
            subscription_id=None
    ):

        # -----------------------------
        # UPDATE EVENT FAILURE STATE
        # -----------------------------

        event.retry_count += 1
        event.failure_type = failure_type.value
        event.last_error = error_message

        retryable = FailureClassifier.is_retryable(
            failure_type
        )

        event.retryable = retryable

        max_retries = RetryPolicy.max_retries(
            failure_type
        )

        # -----------------------------
        # MOVE TO DLQ
        # -----------------------------

        if (
                not retryable
                or event.retry_count > max_retries
        ):
            event.status = "dead"
            event.next_retry_at = None

            DLQService.move_to_dlq(
                db=db,
                event_id=event.id,
                failure_type=failure_type.value,
                final_error=error_message
            )

            db.commit()

            logger.error(
                "Event moved to DLQ",
                extra={"failure_type": failure_type.value, "error": error_message}
            )

            return

        # -----------------------------
        # RETRY DELAY
        # -----------------------------

        delay = RetryPolicy.get_delay(
            failure_type,
            event.retry_count
        )

        retry_time = (
                datetime.now(timezone.utc)
                + timedelta(seconds=delay)
        )

        event.status = "retrying"
        event.next_retry_at = retry_time

        db.commit()

        logger.info(
            f"Webhook scheduled for retry in {delay} seconds",
            extra={"delay_seconds": delay, "failure_type": failure_type.value, "next_retry_at": retry_time.isoformat()}
        )

        # -----------------------------
        # IMPORT INSIDE FUNCTION
        # AVOID CIRCULAR IMPORT
        # -----------------------------

        from worker.tasks import deliver_webhook

        # -----------------------------
        # CHECK CIRCUIT BEFORE RETRY
        # -----------------------------

        from app.services.reliability.circuit_breaker_service import (
            CircuitBreakerService
        )

        target_url = endpoint_url
        if not target_url:
            tenant = db.get(Tenant, event.tenant_id)
            target_url = tenant.webhook_url if tenant else None

        if target_url:

            circuit = CircuitBreakerService.get_or_create(
                db=db,
                endpoint_url=target_url,
                tenant_id=event.tenant_id,
            )

            if circuit.state == "open":
                delay = circuit.cooldown_seconds

                event.status = "circuit_open"
                event.next_retry_at = (
                        datetime.now(timezone.utc)
                        + timedelta(seconds=delay)
                )
                db.commit()

                logger.warning(
                    f"Circuit open during retry setup. Using cooldown delay: {delay}s",
                    extra={"delay_seconds": delay}
                )

        # Determine target queue based on retry count
        target_queue = "default" if event.retry_count <= 2 else "low_priority"

        # -----------------------------
        # SCHEDULE RETRY
        # -----------------------------

        deliver_webhook.apply_async(
            args=[event.id, endpoint_url, subscription_id],
            countdown=delay,
            queue=target_queue
        )