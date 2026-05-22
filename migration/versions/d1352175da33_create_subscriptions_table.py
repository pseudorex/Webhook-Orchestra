"""create subscriptions table

Revision ID: d1352175da33
Revises: cb0c72f9fad6
Create Date: 2026-05-22 11:09:34.181094

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd1352175da33'
down_revision: Union[str, Sequence[str], None] = 'cb0c72f9fad6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.create_table(
        'subscriptions',

        sa.Column('id', sa.Integer(), nullable=False),

        sa.Column('tenant_id', sa.String(), nullable=False),

        sa.Column('topic', sa.String(), nullable=False),

        sa.Column('endpoint_url', sa.String(), nullable=False),

        sa.Column('is_active', sa.Boolean(), nullable=True),

        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=True
        ),

        sa.PrimaryKeyConstraint('id')
    )

    op.create_index(
        op.f('ix_subscriptions_id'),
        'subscriptions',
        ['id'],
        unique=False
    )

    op.create_index(
        op.f('ix_subscriptions_tenant_id'),
        'subscriptions',
        ['tenant_id'],
        unique=False
    )

    op.create_index(
        op.f('ix_subscriptions_topic'),
        'subscriptions',
        ['topic'],
        unique=False
    )    # ### end Alembic commands ###


def downgrade() -> None:

    op.drop_index(
        op.f('ix_subscriptions_topic'),
        table_name='subscriptions'
    )

    op.drop_index(
        op.f('ix_subscriptions_tenant_id'),
        table_name='subscriptions'
    )

    op.drop_index(
        op.f('ix_subscriptions_id'),
        table_name='subscriptions'
    )

    op.drop_table('subscriptions')    # ### end Alembic commands ###
