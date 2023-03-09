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
        SET project_name = owner
        """
    )

    op.alter_column(
        "jobs",
        "project_name",
        nullable=False,
    )


def downgrade() -> None:
    op.drop_column("jobs", "project_name")
