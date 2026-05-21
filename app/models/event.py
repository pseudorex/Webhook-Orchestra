from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean
)

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.sql import func

from app.core.database import Base
from sqlalchemy import UniqueConstraint

class Event(Base):

    __tablename__ = "events"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    tenant_id = Column(
        Integer,
        ForeignKey("tenants.id")
    )

    event_type = Column(
        String,
        nullable=False
    )

    payload = Column(
        JSONB,
        nullable=False
    )

    status = Column(
        String,
        default="received"
    )

    # -------------------------
    # RELIABILITY LAYER
    # -------------------------

    retry_count = Column(
        Integer,
        default=0
    )

    failure_type = Column(
        String,
        nullable=True
    )

    retryable = Column(
        Boolean,
        default=True
    )

    next_retry_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    last_error = Column(
        String,
        nullable=True
    )

    # -------------------------
    # DELIVERY TRACKING
    # -------------------------

    delivered_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    received_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )

    # -------------------------
    # IDEMPOTENCY
    # -------------------------

    idempotency_key = Column(
        String,
        nullable=True
    )

    # -------------------------
    # REPLAY SYSTEM
    # -------------------------

    replay_count = Column(
        Integer,
        default=0
    )

    last_replayed_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    __table_args__ = (
        UniqueConstraint('tenant_id', 'idempotency_key', name='uq_tenant_idempotency'),
    )