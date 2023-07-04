"""add org/project hash column

Revision ID: 631e12f654a4
Revises: 0c26b76ea170
Create Date: 2023-07-03 13:15:13.812736

"""
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg

from alembic import op

# revision identifiers, used by Alembic.
revision = "631e12f654a4"
down_revision = "0c26b76ea170"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("org_project_hash", sapg.BYTEA))
    op.execute(
        """
        UPDATE jobs
        SET org_project_hash = substring(
            digest(
                COALESCE(org_name, 'NO_ORG') || project_name,
                'sha256'
            )
            from 1 for 5
        )
        """
    )
    op.alter_column("jobs", "org_project_hash", nullable=False)

    op.create_index("jobs_org_project_hash_index", "jobs", ["org_project_hash"])


def downgrade() -> None:
    op.drop_index("jobs_org_project_hash_index", "jobs")
    op.drop_column("jobs", "org_project_hash")
