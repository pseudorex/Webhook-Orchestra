from app.models.delivery_attempt import (
    DeliveryAttempt
)


class DeliveryAttemptService:

    @staticmethod
    def log_attempt(
        db,
        event_id,
        attempt_number,
        status_code=None,
        response_body=None,
        response_time_ms=None,
        failure_type=None
    ):

        attempt = DeliveryAttempt(

            event_id=event_id,

            attempt_number=attempt_number,

            status_code=status_code,

            response_body=response_body,

            response_time_ms=response_time_ms,

            failure_type=failure_type
        )

        db.add(attempt)

        db.commit()

        db.refresh(attempt)

        return attempt