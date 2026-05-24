from datetime import datetime, timezone

from app.models.event import Event

from worker.celery_app import celery


class ReplayError(Exception):
    pass


async def replay_event(
    db,
    event_id
):

    # FETCH EVENT
    event = await db.get(Event, event_id)

    if not event:

        raise ReplayError(
            "Event not found"
        )

    # ALLOW ONLY DEAD EVENTS
    if event.status != "dead":

        raise ReplayError(
            "Only dead events can be replayed"
        )

    # UPDATE REPLAY METADATA
    event.replay_count += 1

    event.last_replayed_at = datetime.now(timezone.utc)

    # MOVE BACK TO RETRYING
    event.status = "retrying"

    await db.commit()

    # REQUEUE TASK
    celery.send_task(
        "worker.tasks.deliver_webhook",
        args=[event.id],
        queue="low_priority"
    )

    return {
        "message": "Replay queued successfully",
        "event_id": event.id,
        "replay_count": event.replay_count
    }