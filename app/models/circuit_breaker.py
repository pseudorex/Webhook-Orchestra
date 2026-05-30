from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    ForeignKey,
    UniqueConstraint
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
        index=True # ← Removed unique=True
    )

    tenant_id = Column(
        Integer,
        ForeignKey("tenants.id"), # ← Added ForeignKey constraint
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

    # --------------------------
    # HEALTH TRACKING METRICS (Phase 5.3)
    # --------------------------

    success_count = Column(
        Integer,
        default=0,
        nullable=False
    )

    total_requests = Column(
        Integer,
        default=0,
        nullable=False
    )

    success_rate = Column(
        Float,
        default=100.0,
        nullable=False
    )

    average_latency_ms = Column(
        Float,
        default=0.0,
        nullable=False
    )

    consecutive_failures = Column(
        Integer,
        default=0,
        nullable=False
    )

    health_score = Column(
        Float,
        default=100.0,
        nullable=False
    )

    health_state = Column(
        String,
        default="healthy",
        nullable=False
    )

    # Composite unique constraint to ensure endpoint is unique per tenant
    __table_args__ = (
        UniqueConstraint("tenant_id", "endpoint_url", name="uq_tenant_endpoint"),
    )