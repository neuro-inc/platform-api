"""add materialized field

Revision ID: 0002
Revises: 0001
Create Date: 2020-10-26 11:40:49.112880

"""

from alembic import op

# revision identifiers, used by Alembic.


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """\
UPDATE jobs
SET payload = payload - 'is_deleted'
 || jsonb_build_object('materialized', NOT (payload->>'is_deleted')::boolean)
WHERE payload ? 'is_deleted'
"""
    )


def downgrade() -> None:
    op.execute(
        """\
UPDATE jobs
SET payload = payload - 'materialized'
 || jsonb_build_object('is_deleted', NOT (payload->>'materialized')::boolean)
WHERE payload ? 'materialized'
"""
    )
