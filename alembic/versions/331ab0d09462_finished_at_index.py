"""finished_at_index

Revision ID: 331ab0d09462
Revises: 68197d86eb6f
Create Date: 2020-12-24 16:15:44.297184

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "331ab0d09462"
down_revision = "68197d86eb6f"
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
