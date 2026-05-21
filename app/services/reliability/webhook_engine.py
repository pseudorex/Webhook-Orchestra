import requests
import time

from datetime import (
    datetime,
    timedelta
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


class WebhookEngine:

    @staticmethod
    def process_event(db, event):

        start = time.time()

        try:

            # -----------------------------
            # FETCH TENANT
            # -----------------------------

            tenant = db.get(
                Tenant,
                event.tenant_id
            )

            if not tenant:

                raise Exception(
                    "TENANT NOT FOUND"
                )

            if not tenant.webhook_url:

                raise Exception(
                    "WEBHOOK URL NOT CONFIGURED"
                )

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
            # SEND WEBHOOK
            # -----------------------------

            response = requests.post(
                tenant.webhook_url,
                json=payload,
                headers={
                    "X-Webhook-Signature": signature,
                    "X-Event-ID": str(event.id)
                },
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

                event.delivered_at = datetime.utcnow()

                event.last_error = None

                event.failure_type = None

                event.retryable = True

                event.next_retry_at = None

                db.commit()

                print("=================================")
                print("WEBHOOK DELIVERED")
                print("STATUS:", response.status_code)
                print("=================================")

                return

            # -----------------------------
            # FAILURE CLASSIFICATION
            # -----------------------------

            failure_type = FailureClassifier.classify(
                status_code=response.status_code
            )

            WebhookEngine.handle_failure(
                db=db,
                event=event,
                failure_type=failure_type,
                error_message=response.text
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

            # -----------------------------
            # HANDLE FAILURE
            # -----------------------------

            WebhookEngine.handle_failure(
                db=db,
                event=event,
                failure_type=failure_type,
                error_message=str(error)
            )

    @staticmethod
    def handle_failure(
        db,
        event,
        failure_type,
        error_message
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

            print("=================================")
            print("EVENT MOVED TO DLQ")
            print("FAILURE TYPE:", failure_type.value)
            print("ERROR:", error_message)
            print("=================================")

            return

        # -----------------------------
        # RETRY DELAY
        # -----------------------------

        delay = RetryPolicy.get_delay(
            failure_type,
            event.retry_count
        )

        retry_time = (
            datetime.utcnow()
            + timedelta(seconds=delay)
        )

        event.status = "retrying"

        event.next_retry_at = retry_time

        db.commit()

        print("=================================")
        print(
            f"RETRYING EVENT IN {delay} SECONDS"
        )
        print("FAILURE TYPE:", failure_type.value)
        print("NEXT RETRY:", retry_time)
        print("=================================")

        # -----------------------------
        # IMPORT INSIDE FUNCTION
        # AVOID CIRCULAR IMPORT
        # -----------------------------

        from worker.tasks import deliver_webhook

        # -----------------------------
        # SCHEDULE RETRY
        # -----------------------------

        deliver_webhook.apply_async(
            args=[event.id],
            countdown=delay
        )