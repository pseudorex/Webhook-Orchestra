from pydantic import BaseModel
from typing import Dict, Any


class EventCreate(BaseModel):

    event_type: str
    payload: Dict[str, Any]
    idempotency_key: str | None = None

class EventResponse(BaseModel):

    id: int
    tenant_id: int
    event_type: str
    payload: Dict[str, Any]
    status: str
    class Config:

        from_attributes = True