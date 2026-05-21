from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey
)

from sqlalchemy.sql import func

from app.core.database import Base


class DeliveryAttempt(Base):

    __tablename__ = "delivery_attempts"

    id = Column(Integer, primary_key=True, index=True)

    event_id = Column(
        Integer,
        ForeignKey("events.id")
    )

    attempt_number = Column(Integer)

    status_code = Column(Integer, nullable=True)

    response_body = Column(String, nullable=True)

    response_time_ms = Column(Integer, nullable=True)

    failure_type = Column(String, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )