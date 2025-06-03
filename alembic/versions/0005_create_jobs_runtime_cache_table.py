"""create jobs runtime cache table

Revision ID: 0005
Revises: 0004
Create Date: 2020-11-30 12:49:26.694858

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs_runtime_cache",
        sa.Column("owner", sa.String(), primary_key=True),
        sa.Column(
            "last_finished",
            sapg.TIMESTAMP(timezone=True, precision=6),
            nullable=False,
        ),
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("jobs_runtime_cache")
