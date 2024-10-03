"""remove jobs_fully_billed_index

Revision ID: 13c284f200c9
Revises: 631e12f654a4
Create Date: 2024-10-03 09:52:28.579238

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "13c284f200c9"
down_revision = "631e12f654a4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("jobs_fully_billed_index", table_name="jobs")


def downgrade() -> None:
    op.create_index(
        "jobs_fully_billed_index",
        "jobs",
        [sa.text("(((payload ->> 'fully_billed'::text))::boolean)")],
    )
