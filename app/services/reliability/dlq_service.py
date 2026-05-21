from app.models.dead_letter_event import DeadLetterEvent


class DLQService:

    @staticmethod
    def move_to_dlq(
        db,
        event_id,
        failure_type,
        final_error
    ):

        dead_event = DeadLetterEvent(
            original_event_id=event_id,
            failure_type=failure_type,
            final_error=final_error
        )

        db.add(dead_event)

        db.commit()

        db.refresh(dead_event)

        return dead_event