"""add retention index

Revision ID: 0ee42d5f1908
Revises: 1497c0e2f5a2
Create Date: 2021-07-09 16:46:45.289722

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "0ee42d5f1908"
down_revision = "1497c0e2f5a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "jobs_being_dropped_index",
        "jobs",
        [sa.text("(((payload ->> 'being_dropped'::text))::boolean)")],
    )


def downgrade() -> None:
    op.drop_index("jobs_being_dropped_index", table_name="jobs")
