"""indexes

Revision ID: bff7b93a59c2
Revises: 81675d81a9c7
Create Date: 2020-10-28 15:52:26.052408

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "bff7b93a59c2"
down_revision = "e62854f697b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Indexes:
    op.create_index(
        "jobs_owner_index",
        "jobs",
        ["owner"],
    )
    op.create_index(
        "jobs_name_index",
        "jobs",
        ["name"],
    )
    op.create_index(
        "jobs_status_index",
        "jobs",
        ["status"],
    )
    op.create_index(
        "jobs_created_at_index",
        "jobs",
        ["created_at"],
    )
    op.create_index(
        "jobs_materialized_index",
        "jobs",
        [sa.text("(((payload ->> 'materialized'::text))::boolean)")],
        postgresql_where=sa.text("(((payload ->> 'materialized'::text))::boolean)"),
    )
    op.execute("CREATE INDEX jobs_tags_index ON jobs USING GIN (tags jsonb_path_ops);")


def downgrade() -> None:
    op.drop_index("jobs_owner_index", table_name="jobs")
    op.drop_index("jobs_name_index", table_name="jobs")
    op.drop_index("jobs_status_index", table_name="jobs")
    op.drop_index("jobs_created_at_index", table_name="jobs")
    op.drop_index("jobs_materialized_index", table_name="jobs")
    op.drop_index("jobs_tags_index", table_name="jobs")
