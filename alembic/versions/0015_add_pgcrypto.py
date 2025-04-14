"""add pgcrypto

Revision ID: 0015
Revises: 0014
Create Date: 2023-07-04 21:04:06.933987

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")


def downgrade() -> None:
    op.execute("DROP EXTENSION pgcrypto")
