from pydantic import BaseModel


class SubscriptionCreate(BaseModel):
    tenant_id: int
    topic: str
    endpoint_url: str


class SubscriptionResponse(BaseModel):
    id: int
    tenant_id: int
    topic: str
    endpoint_url: str
    is_active: bool

    class Config:
        from_attributes = True