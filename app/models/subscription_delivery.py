from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from app.core.database import Base

# Added comments to mark custom models
# SubscriptionDelivery tracks the delivery state of a specific event to a specific subscription
class SubscriptionDelivery(Base):
    __tablename__ = "subscription_deliveries"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    subscription_id = Column(Integer, ForeignKey("subscriptions.id"), nullable=False)
    status = Column(String, default="pending", nullable=False) # pending, retrying, delivered, dead, circuit_open
    retry_count = Column(Integer, default=0, nullable=False)
    failure_type = Column(String, nullable=True)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(String, nullable=True)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
