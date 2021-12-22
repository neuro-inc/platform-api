"""drop jobs runtime cache table

Revision ID: de3a85c9c87a
Revises: 1497c0e2f5a2
Create Date: 2021-07-07 17:12:57.615587

"""
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "de3a85c9c87a"
down_revision = "0ee42d5f1908"
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
