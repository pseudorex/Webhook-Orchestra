from fastapi import Header, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.tenant import Tenant
from app.api.dependencies.database import get_db
from app.core.logging import tenant_id_var


async def get_current_tenant(
    x_api_key: str = Header(...),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(Tenant).where(
            Tenant.api_key == x_api_key
        )
    )

    tenant = result.scalar_one_or_none()

    if not tenant:
        raise HTTPException(
            status_code=401,
            detail="Invalid API Key"
        )

    # Save authenticated tenant ID in logging context
    tenant_id_var.set(tenant.id)

    return tenant