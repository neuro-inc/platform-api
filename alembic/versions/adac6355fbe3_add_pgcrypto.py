"""add pgcrypto

Revision ID: adac6355fbe3
Revises: 0c26b76ea170
Create Date: 2023-07-04 21:04:06.933987

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "adac6355fbe3"
down_revision = "0c26b76ea170"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")


def downgrade() -> None:
    op.execute("DROP EXTENSION pgcrypto")
