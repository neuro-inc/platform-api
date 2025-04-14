"""change materialized index

Revision ID: 0004
Revises: 0003
Create Date: 2020-11-27 16:33:40.235283

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("jobs_materialized_index", table_name="jobs")
    op.create_index(
        "jobs_materialized_index",
        "jobs",
        [sa.text("(((payload ->> 'materialized'::text))::boolean)")],
    )
    op.execute("ANALYZE jobs;")  # Collect statistics.


def downgrade() -> None:
    op.drop_index("jobs_materialized_index", table_name="jobs")
    op.create_index(
        "jobs_materialized_index",
        "jobs",
        [sa.text("(((payload ->> 'materialized'::text))::boolean)")],
        postgresql_where=sa.text("(((payload ->> 'materialized'::text))::boolean)"),
    )
