"""base owner

Revision ID: 1497c0e2f5a2
Revises: 5a3bdd81e17d
Create Date: 2021-06-09 17:54:19.658086

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "1497c0e2f5a2"
down_revision = "5a3bdd81e17d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Index to simulate conditional unique constraint
    op.drop_index("jobs_name_owner_uq")
    op.create_index(
        "jobs_name_owner_uq",
        "jobs",
        ["name", sa.text("split_part(owner, '/', 1)")],
        unique=True,
        postgresql_where=sa.text(
            "(jobs.status != 'succeeded' AND jobs.status != 'failed' AND jobs.status != 'cancelled')"  # noqa
        ),
    )
    op.create_index(
        "jobs_base_owner_index",
        "jobs",
        [sa.text("split_part(owner, '/', 1)")],
    )


def downgrade() -> None:
    op.drop_index("jobs_base_owner_index")
    op.drop_index("jobs_name_owner_uq")
    op.create_index(
        "jobs_name_owner_uq",
        "jobs",
        ["name", "owner"],
        unique=True,
        postgresql_where=sa.text(
            "(jobs.status != 'succeeded' AND jobs.status != 'failed' AND jobs.status != 'cancelled')"  # noqa
        ),
    )
