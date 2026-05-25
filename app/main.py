from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import uuid
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.core.metrics import REGISTRY, update_queue_length_metrics

from app.core.logging import (
    setup_app_logging,
    correlation_id_var,
    tenant_id_var,
    event_id_var
)

# Initialize logging before FastAPI boots
setup_app_logging()

from app.api.routes.tenants import router as tenant_router
from app.api.routes.events import router as event_router
from app.api.routes.webhook_receiver import router as webhook_router
from app.api.routes.replay import router as replay_router
from app.api.routes.dlq import router as dlq_router
from app.api.routes.subscription import router as subscription_router
from app.api.routes.endpoints import router as endpoint_router

app = FastAPI(title="Webhook Orchestra")


class LoggingContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        corr_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())

        # Set thread-local context variables for this request
        corr_token = correlation_id_var.set(corr_id)
        tenant_token = tenant_id_var.set(None)
        event_token = event_id_var.set(None)

        try:
            response = await call_next(request)
            # Return correlation ID to client
            response.headers["X-Correlation-ID"] = corr_id
            return response
        finally:
            # Clean up context to prevent thread leakages
            correlation_id_var.reset(corr_token)
            tenant_id_var.reset(tenant_token)
            event_id_var.reset(event_token)


app.add_middleware(LoggingContextMiddleware)

app.include_router(tenant_router)
app.include_router(event_router)
app.include_router(webhook_router)
app.include_router(replay_router)
app.include_router(dlq_router)
app.include_router(subscription_router)
app.include_router(endpoint_router)

@app.get("/metrics")
def get_metrics():
    # Update Redis queue length gauges dynamically right before scraping
    update_queue_length_metrics()
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST
    )


@app.get("/")
def root():
    return {
        "message": "Webhook Orchestra Running"
    }