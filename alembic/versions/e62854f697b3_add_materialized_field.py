"""add materialized field

Revision ID: e62854f697b3
Revises: 81675d81a9c7
Create Date: 2020-10-26 11:40:49.112880

"""
from alembic import op

# revision identifiers, used by Alembic.


revision = "e62854f697b3"
down_revision = "eaa33ba10d63"
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
