from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.event import Event


async def get_dead_events(
    db: AsyncSession,
    limit: int = 10,
    offset: int = 0
):

    result = await db.execute(
        select(Event)
        .where(Event.status == "dead")
        .offset(offset)
        .limit(limit)
        .order_by(Event.received_at.desc())
    )

    events = result.scalars().all()

    return events


async def get_event_by_id(
    db: AsyncSession,
    event_id: int
):

    event = await db.get(Event, event_id)

    return event