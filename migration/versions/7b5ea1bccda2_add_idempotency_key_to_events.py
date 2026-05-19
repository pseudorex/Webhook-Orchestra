"""add idempotency key to events"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '7b5ea1bccda2'
down_revision: Union[str, Sequence[str], None] = 'e6f028aeb00c'
branch_labels = None
depends_on = None


def upgrade() -> None:

    op.add_column(
        'events',
        sa.Column(
            'idempotency_key',
            sa.String(),
            nullable=True
        )
    )

    op.create_unique_constraint(
        None,
        'events',
        ['idempotency_key']
    )


def downgrade() -> None:

    op.drop_constraint(
        None,
        'events',
        type_='unique'
    )

    op.drop_column(
        'events',
        'idempotency_key'
    )