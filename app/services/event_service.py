from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.event import Event


async def get_dead_events(
    db: AsyncSession,
    tenant_id: int,
    limit: int = 10,
    offset: int = 0
):
    result = await db.execute(
        select(Event)
        .where(
            Event.status == "dead",
            Event.tenant_id == tenant_id
        )
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