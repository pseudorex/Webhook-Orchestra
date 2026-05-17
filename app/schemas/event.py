from pydantic import BaseModel
from typing import Dict, Any


class EventCreate(BaseModel):

    event_type: str

    payload: Dict[str, Any]


class EventResponse(BaseModel):

    id: int

    tenant_id: int

    event_type: str

    payload: Dict[str, Any]

    status: str

    class Config:

        from_attributes = True