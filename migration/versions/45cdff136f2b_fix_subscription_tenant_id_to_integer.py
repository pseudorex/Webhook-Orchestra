"""fix subscription tenant_id to integer

Revision ID: 45cdff136f2b
Revises: d1352175da33
Create Date: 2026-05-22 11:54:31.748668

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '45cdff136f2b'
down_revision: Union[str, Sequence[str], None] = 'd1352175da33'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'subscriptions',
        'tenant_id',
        existing_type=sa.VARCHAR(),
        type_=sa.Integer(),
        existing_nullable=False,
        postgresql_using='tenant_id::integer'
    )


def downgrade() -> None:
    op.alter_column(
        'subscriptions',
        'tenant_id',
        existing_type=sa.Integer(),
        type_=sa.VARCHAR(),
        existing_nullable=False
    )