"""add delivery tracking fields

Revision ID: f44883f72b98
Revises: 11a758cbad00
Create Date: 2026-05-17 10:48:42.761175
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f44883f72b98'
down_revision: Union[str, Sequence[str], None] = '11a758cbad00'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.add_column(
        'events',
        sa.Column('retry_count', sa.Integer(), nullable=True)
    )

    op.add_column(
        'events',
        sa.Column('last_error', sa.String(), nullable=True)
    )

    op.add_column(
        'events',
        sa.Column(
            'delivered_at',
            sa.DateTime(timezone=True),
            nullable=True
        )
    )

    op.alter_column(
        'events',
        'tenant_id',
        existing_type=sa.INTEGER(),
        nullable=True
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.alter_column(
        'events',
        'tenant_id',
        existing_type=sa.INTEGER(),
        nullable=False
    )

    op.drop_column('events', 'delivered_at')

    op.drop_column('events', 'last_error')

    op.drop_column('events', 'retry_count')