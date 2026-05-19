"""create processed webhooks table

Revision ID: e6f028aeb00c
Revises: 1261d0b1bb96
Create Date: 2026-05-19 11:45:11.234441
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision: str = 'e6f028aeb00c'
down_revision: Union[str, Sequence[str], None] = '1261d0b1bb96'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.create_table(
        'processed_webhooks',

        sa.Column(
            'id',
            sa.Integer(),
            primary_key=True
        ),

        sa.Column(
            'event_id',
            sa.String(),
            nullable=False,
            unique=True
        ),

        sa.Column(
            'processed_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now()
        )
    )


def downgrade() -> None:

    op.drop_table('processed_webhooks')