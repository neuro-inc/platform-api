"""finished_at_index

Revision ID: 0007
Revises: 0006
Create Date: 2020-12-24 16:15:44.297184

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "jobs_finished_at_index",
        "jobs",
        ["finished_at"],
    )


def downgrade() -> None:
    op.drop_index("jobs_finished_at_index", table_name="jobs")
