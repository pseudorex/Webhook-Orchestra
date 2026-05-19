from fastapi import (
    APIRouter,
    Depends
)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.schemas.event import (
    EventCreate,
    EventResponse
)

from app.models.event import Event
from app.models.tenant import Tenant

from app.api.dependencies.database import get_db
from app.api.dependencies.auth import get_current_tenant

from worker.tasks import deliver_webhook


router = APIRouter(
    prefix="/events",
    tags=["Events"]
)


@router.post(
    "/",
    response_model=EventResponse
)
async def create_event(
    event: EventCreate,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):

    # IDEMPOTENCY CHECK
    if event.idempotency_key:

        existing_event = await db.execute(
            select(Event).where(
                Event.idempotency_key == event.idempotency_key
            )
        )

        existing_event = existing_event.scalar_one_or_none()

        # RETURN EXISTING EVENT
        if existing_event:

            return existing_event

    # CREATE NEW EVENT
    new_event = Event(
        tenant_id=tenant.id,
        event_type=event.event_type,
        payload=event.payload,
        status="received",

        # NEW FIELD
        idempotency_key=event.idempotency_key
    )

    db.add(new_event)

    await db.commit()

    await db.refresh(new_event)

    # PUSH EVENT TO QUEUE
    deliver_webhook.delay(new_event.id)

    return new_event