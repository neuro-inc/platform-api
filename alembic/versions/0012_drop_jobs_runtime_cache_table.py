"""drop jobs runtime cache table

Revision ID: 0012
Revises: 0011
Create Date: 2021-07-07 17:12:57.615587

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("jobs_runtime_cache")


def downgrade() -> None:
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
