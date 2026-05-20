"""add replay metadata

Revision ID: 44a6e780b64d
Revises: 7b5ea1bccda2
Create Date: 2026-05-20 10:57:22.404063

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '44a6e780b64d'
down_revision: Union[str, Sequence[str], None] = '7b5ea1bccda2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column(
        "events",
        sa.Column("replay_count", sa.Integer(), nullable=False, server_default="0")
    )

    op.add_column(
        "events",
        sa.Column("last_replayed_at", sa.DateTime(), nullable=True)
    )


def downgrade():
    op.drop_column("events", "replay_count")

    op.drop_column("events", "last_replayed_at")
