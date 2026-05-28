from fastapi import APIRouter, Depends
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.schemas.tenant import (
    TenantCreate,
    TenantResponse
)
import secrets
from app.models.tenant import Tenant
from app.core.security import generate_api_key
from app.api.dependencies.database import get_db
from app.api.dependencies.auth import get_current_tenant


router = APIRouter(
    prefix="/tenants",
    tags=["Tenants"]
)


@router.post(
    "/register",
    response_model=TenantResponse
)
async def register_tenant(
    tenant: TenantCreate,
    db: AsyncSession = Depends(get_db)
):

    existing_tenant = await db.execute(
        select(Tenant).where(
            Tenant.email == tenant.email
        )
    )

    existing_tenant = existing_tenant.scalar_one_or_none()

    if existing_tenant:
        raise HTTPException(
            status_code=400,
            detail="Tenant already exists"
        )

    new_tenant = Tenant(
        name=tenant.name,
        email=tenant.email,
        api_key=generate_api_key(),
        webhook_url=tenant.webhook_url,
        webhook_secret=secrets.token_hex(32)
    )

    db.add(new_tenant)
    await db.commit()
    await db.refresh(new_tenant)
    return new_tenant


@router.get("/me", response_model=TenantResponse)
async def get_my_tenant(
    tenant: Tenant = Depends(get_current_tenant)
):
    return tenant