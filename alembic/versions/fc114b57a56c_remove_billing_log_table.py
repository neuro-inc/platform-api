"""remove billing_log table

Revision ID: fc114b57a56c
Revises: 13c284f200c9
Create Date: 2024-10-03 10:35:09.025409

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "fc114b57a56c"
down_revision = "13c284f200c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("billing_log")
    op.drop_table("sync_record")


def downgrade() -> None:
    op.create_table(
        "billing_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("job_id", sa.String(), nullable=False),
        # All other fields
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )
    op.create_table(
        "sync_record",
        sa.Column("type", sa.String(), primary_key=True),
        sa.Column("last_entry_id", sa.Integer()),
    )
    op.create_index(
        "billing_log_job_index",
        "billing_log",
        ["job_id"],
    )
