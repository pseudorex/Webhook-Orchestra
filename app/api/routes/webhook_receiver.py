from fastapi import APIRouter, Request, Header, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.processed_webhook import ProcessedWebhook
from app.models.event import Event # ← Imported Event model
from app.models.tenant import Tenant # ← Imported Tenant model
from app.api.dependencies.database import get_db           # ← use the async dep
from app.services.signature_service import generate_signature

router = APIRouter()


@router.post("/webhook")
async def receive_webhook(
    request: Request,
    x_event_id: str = Header(...),
    x_webhook_signature: str = Header(None),                # ← accept signature
    db: AsyncSession = Depends(get_db)                      # ← inject async session
):
    payload = await request.json()

    # Retrieve the event and tenant context to get the webhook secret for signature verification
    try:
        event_id_int = int(x_event_id)
        result = await db.execute(
            select(Event).where(Event.id == event_id_int)
        )
        event = result.scalar_one_or_none()
        if not event:
            raise HTTPException(status_code=400, detail="Original event not found for signature lookup")
        
        result = await db.execute(
            select(Tenant).where(Tenant.id == event.tenant_id)
        )
        tenant = result.scalar_one_or_none()
        if not tenant or not tenant.webhook_secret:
            raise HTTPException(status_code=400, detail="Tenant webhook secret not found")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Event ID format")

    # VALIDATE SIGNATURE
    expected_signature = generate_signature(payload=payload, secret=tenant.webhook_secret)
    if x_webhook_signature != expected_signature:
        raise HTTPException(status_code=401, detail="Invalid Webhook Signature")

    # CHECK DUPLICATE
    result = await db.execute(
        select(ProcessedWebhook).where(
            ProcessedWebhook.event_id == x_event_id
        )
    )
    existing_event = result.scalar_one_or_none()

    if existing_event:
        return {"message": "Duplicate ignored"}

    # STORE PROCESSED EVENT
    processed_event = ProcessedWebhook(event_id=x_event_id)
    db.add(processed_event)
    await db.commit()

    return {"message": "Webhook processed successfully"}