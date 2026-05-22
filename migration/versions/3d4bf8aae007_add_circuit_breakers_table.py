"""add circuit_breakers table

Revision ID: 3d4bf8aae007
Revises: 45cdff136f2b
Create Date: 2026-05-22 12:22:57.341581

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3d4bf8aae007'
down_revision: Union[str, Sequence[str], None] = '45cdff136f2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'circuit_breakers',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('endpoint_url', sa.String(), nullable=False),
        sa.Column('tenant_id', sa.Integer(), nullable=False),
        sa.Column('state', sa.String(), nullable=False),
        sa.Column('failure_count', sa.Integer(), nullable=True),
        sa.Column('failure_threshold', sa.Integer(), nullable=True),
        sa.Column('cooldown_seconds', sa.Integer(), nullable=True),
        sa.Column('last_failure_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('opened_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_success_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_circuit_breakers_id'), 'circuit_breakers', ['id'], unique=False)
    op.create_index(op.f('ix_circuit_breakers_endpoint_url'), 'circuit_breakers', ['endpoint_url'], unique=True)
    op.create_index(op.f('ix_circuit_breakers_tenant_id'), 'circuit_breakers', ['tenant_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_circuit_breakers_tenant_id'), table_name='circuit_breakers')
    op.drop_index(op.f('ix_circuit_breakers_endpoint_url'), table_name='circuit_breakers')
    op.drop_index(op.f('ix_circuit_breakers_id'), table_name='circuit_breakers')
    op.drop_table('circuit_breakers')
