from sqlalchemy import select

from app.schemas.event import (
    EventCreate,
    EventResponse
)

from app.models.event import Event
from app.models.tenant import Tenant

from app.api.dependencies.database import get_db
from app.api.dependencies.auth import get_current_tenant

from app.services.routing_engine import RoutingEngine

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.event_service import (
    get_dead_events,
    get_event_by_id
)
from app.core.logging import event_id_var


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
                Event.idempotency_key == event.idempotency_key,
                Event.tenant_id == tenant.id
            )
        )

        existing_event = existing_event.scalar_one_or_none()

        # RETURN EXISTING EVENT
        if existing_event:
            # Set context variable for existing event before exiting
            event_id_var.set(existing_event.id)
            return existing_event

    # CREATE NEW EVENT
    new_event = Event(
        tenant_id=tenant.id,
        event_type=event.event_type,
        payload=event.payload,
        status="received",
        idempotency_key=event.idempotency_key
    )

    db.add(new_event)
    await db.commit()
    await db.refresh(new_event)

    # Set context variable for the new event
    event_id_var.set(new_event.id)

    # FAN-OUT TO ALL SUBSCRIPTIONS
    await RoutingEngine.fan_out_event(
        db=db,
        event=new_event
    )

    return new_event


@router.get("/dead")
async def dead_events(
    limit: int = 10,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    events = await get_dead_events(
        db=db,
        limit=limit,
        offset=offset
    )
    return events


@router.get("/{event_id}")
async def get_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    event = await get_event_by_id(db=db, event_id=event_id)
    if not event or event.tenant_id != tenant.id:
        raise HTTPException(status_code=404, detail="Event not found")
    # Set context variables when retrieving event details
    event_id_var.set(event.id)
    return event