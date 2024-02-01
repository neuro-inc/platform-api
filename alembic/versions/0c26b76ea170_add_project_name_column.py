"""add project name column

Revision ID: 0c26b76ea170
Revises: ca2963935742
Create Date: 2023-03-09 12:48:50.668287

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0c26b76ea170"
down_revision = "ca2963935742"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("project_name", sa.String()),
    )
    op.execute(
        """
        UPDATE jobs
        SET project_name = split_part(owner, '/', 1)
        """
    )
    op.alter_column(
        "jobs",
        "project_name",
        nullable=False,
    )

    op.drop_index("jobs_name_owner_uq")
    op.create_index(
        "jobs_name_project_name_uq",
        "jobs",
        ["name", "project_name"],
        unique=True,
        postgresql_where=sa.text(
            "(jobs.status != 'succeeded' AND jobs.status != 'failed' AND jobs.status != 'cancelled')"  # noqa
        ),
    )


def downgrade() -> None:
    op.drop_index("jobs_name_project_name_uq")
    op.create_index(
        "jobs_name_owner_uq",
        "jobs",
        ["name", sa.text("split_part(owner, '/', 1)")],
        unique=True,
        postgresql_where=sa.text(
            "(jobs.status != 'succeeded' AND jobs.status != 'failed' AND jobs.status != 'cancelled')"  # noqa
        ),
    )

    op.drop_column("jobs", "project_name")
