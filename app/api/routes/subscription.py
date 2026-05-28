from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies.database import get_db
from app.api.dependencies.auth import get_current_tenant
from app.models.tenant import Tenant
from app.schemas.subscription import (
    SubscriptionCreate,
    SubscriptionResponse,
)
from app.services.subscription_service import SubscriptionCRUD

router = APIRouter(prefix="/subscriptions", tags=["Subscriptions"])


@router.post("/", response_model=SubscriptionResponse)
async def create_subscription(
    payload: SubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    if payload.tenant_id != tenant.id:
        raise HTTPException(
            status_code=403,
            detail="Forbidden: Cannot create subscriptions for another tenant"
        )
    return await SubscriptionCRUD.create_subscription(db, payload)