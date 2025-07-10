"""add org_name column

Revision ID: 0013
Revises: 0012
Create Date: 2021-11-17 16:10:58.573317

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("org_name", sa.String()),
    )
    op.create_index(
        "jobs_orgs_index",
        "jobs",
        ["org_name"],
    )


def downgrade() -> None:
    op.drop_index("jobs_orgs_index", "jobs")
    op.drop_column("jobs", "org_name")
