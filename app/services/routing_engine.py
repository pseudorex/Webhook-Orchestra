from sqlalchemy.ext.asyncio import AsyncSession

from app.services.subscription_service import SubscriptionCRUD
from worker.tasks import deliver_webhook


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

            deliver_webhook.delay(
                event_id=event.id,
                endpoint_url=subscription.endpoint_url,
                subscription_id=subscription.id,
            )