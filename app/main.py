from fastapi import FastAPI
from app.api.routes.tenants import (
    router as tenant_router
)
from app.api.routes.events import (
    router as event_router
)
from app.api.routes.webhook_receiver import (
    router as webhook_router
)
from app.api.routes.replay import (
    router as replay_router
)

from app.api.routes.dlq import (
    router as dlq_router
)

from app.api.routes.subscription import (
    router as subscription_router
)

app = FastAPI(
    title="Webhook Orchestra"
)

app.include_router(tenant_router)
app.include_router(event_router)
app.include_router(webhook_router)
app.include_router(replay_router)
app.include_router(dlq_router)
app.include_router(subscription_router)


@app.get("/")
def root():

    return {
        "message": "Webhook Orchestra Running"
    }