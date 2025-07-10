"""

Add job namespace

Revision ID: 0019
Revises: fc114b57a56c
Create Date: 2025-03-26 15:39:55.555506

"""

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

# revision identifiers, used by Alembic.
revision = "0019"
down_revision = "fc114b57a56c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("namespace", sa.String(), nullable=True))
    op.execute(
        text(
            """
            UPDATE jobs
            SET namespace='platform-jobs'
            """
        )
    )


def downgrade() -> None:
    op.drop_column("jobs", "namespace")
