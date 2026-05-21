"""phase_4_reliability_layer

Revision ID: cb0c72f9fad6
Revises: 44a6e780b64d
Create Date: 2026-05-21 11:38:16.310834

"""

from typing import Sequence, Union

from alembic import op

import sqlalchemy as sa

from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.

revision: str = 'cb0c72f9fad6'

down_revision: Union[str, Sequence[str], None] = '44a6e780b64d'

branch_labels: Union[str, Sequence[str], None] = None

depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # -----------------------------
    # EVENTS TABLE RELIABILITY FIELDS
    # -----------------------------

    op.add_column(
        'events',
        sa.Column(
            'failure_type',
            sa.String(),
            nullable=True
        )
    )

    op.add_column(
        'events',
        sa.Column(
            'retryable',
            sa.Boolean(),
            nullable=True
        )
    )

    op.add_column(
        'events',
        sa.Column(
            'next_retry_at',
            sa.DateTime(timezone=True),
            nullable=True
        )
    )

    # -----------------------------
    # ALTER EXISTING COLUMNS
    # -----------------------------

    op.alter_column(
        'events',
        'replay_count',
        existing_type=sa.INTEGER(),
        nullable=True,
        existing_server_default=sa.text('0')
    )

    op.alter_column(
        'events',
        'last_replayed_at',
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True
    )

    # -----------------------------
    # DELIVERY ATTEMPTS TABLE
    # -----------------------------

    op.create_table(
        'delivery_attempts',

        sa.Column(
            'id',
            sa.Integer(),
            primary_key=True
        ),

        sa.Column(
            'event_id',
            sa.Integer(),
            sa.ForeignKey('events.id')
        ),

        sa.Column(
            'attempt_number',
            sa.Integer(),
            nullable=False
        ),

        sa.Column(
            'status_code',
            sa.Integer(),
            nullable=True
        ),

        sa.Column(
            'response_body',
            sa.String(),
            nullable=True
        ),

        sa.Column(
            'response_time_ms',
            sa.Integer(),
            nullable=True
        ),

        sa.Column(
            'failure_type',
            sa.String(),
            nullable=True
        ),

        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()')
        )
    )

    # -----------------------------
    # DEAD LETTER EVENTS TABLE
    # -----------------------------

    op.create_table(
        'dead_letter_events',

        sa.Column(
            'id',
            sa.Integer(),
            primary_key=True
        ),

        sa.Column(
            'original_event_id',
            sa.Integer(),
            nullable=False
        ),

        sa.Column(
            'failure_type',
            sa.String(),
            nullable=False
        ),

        sa.Column(
            'final_error',
            sa.String(),
            nullable=False
        ),

        sa.Column(
            'replay_count',
            sa.Integer(),
            server_default='0'
        ),

        sa.Column(
            'failed_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()')
        )
    )


def downgrade() -> None:
    """Downgrade schema."""

    # -----------------------------
    # DROP NEW TABLES
    # -----------------------------

    op.drop_table('dead_letter_events')

    op.drop_table('delivery_attempts')

    # -----------------------------
    # REMOVE RELIABILITY FIELDS
    # -----------------------------

    op.drop_column(
        'events',
        'next_retry_at'
    )

    op.drop_column(
        'events',
        'retryable'
    )

    op.drop_column(
        'events',
        'failure_type'
    )

    # -----------------------------
    # REVERT EXISTING COLUMNS
    # -----------------------------

    op.alter_column(
        'events',
        'last_replayed_at',
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=True
    )

    op.alter_column(
        'events',
        'replay_count',
        existing_type=sa.INTEGER(),
        nullable=False,
        existing_server_default=sa.text('0')
    )