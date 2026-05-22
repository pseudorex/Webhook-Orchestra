from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.subscription import Subscription
from app.schemas.subscription import SubscriptionCreate


class SubscriptionCRUD:

    @staticmethod
    async def create_subscription(
        db: AsyncSession,
        payload: SubscriptionCreate,
    ):
        subscription = Subscription(
            tenant_id=payload.tenant_id,
            topic=payload.topic,
            endpoint_url=payload.endpoint_url,
        )

        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)

        return subscription

    @staticmethod
    async def get_topic_subscriptions(
        db: AsyncSession,
        tenant_id,
        topic: str,
    ):
        result = await db.execute(
            select(Subscription).where(
                Subscription.tenant_id == tenant_id,
                Subscription.topic == topic,
                Subscription.is_active == True,
            )
        )

        return result.scalars().all()