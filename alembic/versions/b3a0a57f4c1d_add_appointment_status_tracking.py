"""add appointment status tracking

Revision ID: b3a0a57f4c1d
Revises: 9b4912f836fd
Create Date: 2026-03-07 17:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3a0a57f4c1d'
down_revision: Union[str, Sequence[str], None] = '9b4912f836fd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('appointments', sa.Column('previous_status', sa.String(), nullable=True))
    op.add_column(
        'appointments',
        sa.Column('status_changed_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )


def downgrade() -> None:
    op.drop_column('appointments', 'status_changed_at')
    op.drop_column('appointments', 'previous_status')
