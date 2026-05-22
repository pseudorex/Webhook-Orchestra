from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
)

from sqlalchemy.sql import func

from app.core.database import Base


class CircuitBreaker(Base):

    __tablename__ = "circuit_breakers"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    endpoint_url = Column(
        String,
        nullable=False,
        unique=True,
        index=True
    )

    tenant_id = Column(
        Integer,
        nullable=False,
        index=True
    )

    # --------------------------
    # CIRCUIT STATE
    # closed | open | half_open
    # --------------------------

    state = Column(
        String,
        default="closed",
        nullable=False
    )

    # --------------------------
    # FAILURE TRACKING
    # --------------------------

    failure_count = Column(
        Integer,
        default=0
    )

    failure_threshold = Column(
        Integer,
        default=5
    )

    # --------------------------
    # COOLDOWN
    # --------------------------

    cooldown_seconds = Column(
        Integer,
        default=60
    )

    # --------------------------
    # TIMESTAMPS
    # --------------------------

    last_failure_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    opened_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    last_success_at = Column(
        DateTime(timezone=True),
        nullable=True
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )