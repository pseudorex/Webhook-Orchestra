import os
import requests
import logging
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.subscription_service import SubscriptionCRUD
from app.models.subscription_delivery import SubscriptionDelivery # ← Imported SubscriptionDelivery model
from worker.tasks import deliver_webhook

logger = logging.getLogger(__name__)

# Initialize RabbitMQ variables
RABBITMQ_API_URL = os.getenv("RABBITMQ_API_URL", "http://rabbitmq:15672/api/queues/%2F")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")


def get_rabbitmq_queue_length(queue_name: str) -> int:
    """Queries RabbitMQ HTTP Management API to fetch queue message count."""
    try:
        response = requests.get(
            f"{RABBITMQ_API_URL}/{queue_name}",
            auth=(RABBITMQ_USER, RABBITMQ_PASSWORD),
            timeout=1
        )
        if response.status_code == 200:
            return response.json().get("messages", 0)
    except Exception as e:
        logger.error(f"Error querying RabbitMQ API: {e}")
    return 0


def get_adaptive_queue(base_queue: str) -> str:
    """
    Checks the queue length of the high-priority queue in RabbitMQ.
    If it is congested (> 10 items), dynamically demotes
    new events to the default queue to prevent starvation.
    """
    if base_queue == "high_priority":
        try:
            high_len = get_rabbitmq_queue_length("high_priority")
            if high_len > 2000:  # Production congestion threshold of 2000 tasks
                logger.warning(
                    f"HIGH PRIORITY CONGESTED ({high_len} tasks). Routing to default.",
                    extra={"queue": "high_priority"}
                )
                return "default"
        except Exception as e:
            logger.error(f"Error reading RabbitMQ queue length: {e}", exc_info=True)
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
            logger.info(f"No subscriptions found for topic: {event.event_type}")
            return

        for subscription in subscriptions:
            logger.info(
                f"Dispatching event to {subscription.endpoint_url}",
                extra={"subscription_id": subscription.id}
            )

            # Create a SubscriptionDelivery record to track this specific delivery attempt
            delivery = SubscriptionDelivery(
                event_id=event.id,
                subscription_id=subscription.id,
                status="pending"
            )
            db.add(delivery)
            await db.commit()
            await db.refresh(delivery)

            # Determine the queue dynamically
            target_queue = get_adaptive_queue("high_priority")

            # Queue the worker task using the subscription_delivery_id
            deliver_webhook.apply_async(
                args=[delivery.id],
                queue=target_queue
            )