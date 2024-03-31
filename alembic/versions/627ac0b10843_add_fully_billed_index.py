"""add credits billing fields

Revision ID: 627ac0b10843
Revises: 331ab0d09462
Create Date: 2021-02-17 18:42:21.656755

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "627ac0b10843"
down_revision = "331ab0d09462"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "jobs_fully_billed_index",
        "jobs",
        [sa.text("(((payload ->> 'fully_billed'::text))::boolean)")],
    )


def downgrade() -> None:
    op.drop_index("jobs_fully_billed_index", table_name="jobs")
