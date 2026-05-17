from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.core.database import Base


class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(
        Integer,
        ForeignKey("tenants.id")
    )
    event_type = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, default="received")
    retry_count = Column(Integer, default=0)
    last_error = Column(String, nullable=True)
    delivered_at = Column(
        DateTime(timezone=True),
        nullable=True
    )
    received_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )