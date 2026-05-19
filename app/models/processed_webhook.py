from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func

from app.core.database import Base


class ProcessedWebhook(Base):

    __tablename__ = "processed_webhooks"

    id = Column(Integer, primary_key=True)

    event_id = Column(
        String,
        unique=True,
        nullable=False
    )

    processed_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )