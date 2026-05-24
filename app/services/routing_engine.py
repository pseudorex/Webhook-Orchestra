import os
import redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.subscription_service import SubscriptionCRUD
from worker.tasks import deliver_webhook


# Initialize Redis Client to query queue lengths
try:
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=0
    )
except Exception:
    redis_client = None


def get_adaptive_queue(base_queue: str) -> str:
    """
    Checks the queue length of the high-priority queue.
    If it is congested (> 10 items), dynamically demotes
    new events to the default queue to prevent starvation.
    """
    if base_queue == "high_priority" and redis_client:
        try:
            high_len = redis_client.llen("high_priority")
            if high_len > 10:  # Threshold of 10 for easier testing
                print(f"[Adaptive Routing] HIGH PRIORITY CONGESTED ({high_len} tasks). Routing to default.")
                return "default"
        except Exception as e:
            print(f"[Adaptive Routing] Error reading Redis queue length: {e}")
    return base_queue


class RoutingEngine:

    @staticmethod
    async def fan_out_event(
        db: AsyncSession,
        event,
    ):

        subscriptions = (
            await SubscriptionCRUD.get_topic_subscriptions(
                db=db,
                tenant_id=event.tenant_id,
                topic=event.event_type,
            )
        )

        if not subscriptions:

            print(
                f"No subscriptions found for topic: "
                f"{event.event_type}"
            )

            return

        for subscription in subscriptions:

            print(
                f"Dispatching event {event.id} "
                f"to {subscription.endpoint_url}"
            )

            # Determine the queue dynamically
            target_queue = get_adaptive_queue("high_priority")

            deliver_webhook.apply_async(
                args=[event.id, subscription.endpoint_url, subscription.id],
                queue=target_queue
            )