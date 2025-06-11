"""add retention index

Revision ID: 0011
Revises: 0010
Create Date: 2021-07-09 16:46:45.289722

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0011"
down_revision = "0010"
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
