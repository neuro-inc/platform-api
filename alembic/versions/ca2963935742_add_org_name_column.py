"""add org_name column

Revision ID: ca2963935742
Revises: de3a85c9c87a
Create Date: 2021-11-17 16:10:58.573317

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "ca2963935742"
down_revision = "de3a85c9c87a"
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
