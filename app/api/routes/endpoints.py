from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.api.dependencies.auth import get_current_tenant
from app.models.tenant import Tenant

from app.api.dependencies.database import get_db
from app.models.circuit_breaker import CircuitBreaker

router = APIRouter(
    prefix="/endpoints",
    tags=["Endpoint Health"]
)

@router.get("/health")
async def get_endpoints_health(
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    result = await db.execute(
        select(CircuitBreaker).where(CircuitBreaker.tenant_id == tenant.id)
    )
    endpoints = result.scalars().all()
    return [
        {
            "id": ep.id,
            "endpoint_url": ep.endpoint_url,
            "state": ep.state,
            "success_rate": round(ep.success_rate, 2),
            "average_latency_ms": round(ep.average_latency_ms, 2),
            "consecutive_failures": ep.consecutive_failures,
            "health_score": round(ep.health_score, 2),
            "health_state": ep.health_state
        }
        for ep in endpoints
    ]