from pydantic import BaseModel, EmailStr


class TenantCreate(BaseModel):
    name: str
    email: str
    webhook_url: str


class TenantResponse(BaseModel):
    id: int
    name: str
    email: EmailStr
    api_key: str
    webhook_url: str | None
    webhook_secret: str
    events_sent_count: int
    delivery_attempts_count: int
    retries_count: int

    class Config:
        from_attributes = True