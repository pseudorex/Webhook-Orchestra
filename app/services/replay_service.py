from datetime import datetime, timezone

from app.models.event import Event
from app.models.subscription_delivery import SubscriptionDelivery # ← Added SubscriptionDelivery import
from sqlalchemy import select # ← Added select import
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

    # Find dead subscription deliveries for this event
    result = await db.execute(
        select(SubscriptionDelivery).where(
            SubscriptionDelivery.event_id == event_id,
            SubscriptionDelivery.status == "dead"
        )
    )
    deliveries = result.scalars().all()

    if not deliveries:
        raise ReplayError(
            "No dead subscription deliveries found for this event"
        )

    # UPDATE REPLAY METADATA
    event.replay_count += 1
    event.last_replayed_at = datetime.now(timezone.utc)

    # Reset and requeue each dead delivery
    for delivery in deliveries:
        delivery.status = "retrying"
        delivery.retry_count = 0
        delivery.failure_type = None
        delivery.last_error = None

        # REQUEUE TASK
        celery.send_task(
            "worker.tasks.deliver_webhook",
            args=[delivery.id], # ← Pass delivery.id instead of event.id
            queue="low_priority"
        )

    await db.commit()

    return {
        "message": "Replay queued successfully",
        "event_id": event.id,
        "replay_count": event.replay_count,
        "deliveries_replayed": len(deliveries)
    }