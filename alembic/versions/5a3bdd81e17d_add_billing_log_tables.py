"""Add billing log tables

Revision ID: 5a3bdd81e17d
Revises: 627ac0b10843
Create Date: 2021-04-21 15:43:12.420458

"""

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "5a3bdd81e17d"
down_revision = "627ac0b10843"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_table("billing_log")
    op.drop_table("sync_record")
