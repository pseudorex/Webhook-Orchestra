from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey
)

from sqlalchemy.sql import func

from app.core.database import Base


class DeadLetterEvent(Base):

    __tablename__ = "dead_letter_events"

    id = Column(Integer, primary_key=True, index=True)

    original_event_id = Column(Integer)

    subscription_delivery_id = Column(
        Integer,
        ForeignKey("subscription_deliveries.id"),
        nullable=True
    )

    failure_type = Column(String)

    final_error = Column(String)

    replay_count = Column(Integer, default=0)

    failed_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )