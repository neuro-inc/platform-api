"""rename preemptible fields

Revision ID: 0006
Revises: 0005
Create Date: 2020-12-21 14:56:02.437730

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """\
UPDATE jobs
SET payload = payload
 || jsonb_build_object('scheduler_enabled', payload->'is_preemptible')
WHERE payload ? 'is_preemptible'
"""
    )
    op.execute(
        """\
UPDATE jobs
SET payload = payload
 || jsonb_build_object('preemptible_node', payload->'is_preemptible_node_required')
WHERE payload ? 'is_preemptible_node_required'
"""
    )


def downgrade() -> None:
    op.execute(
        """\
UPDATE jobs
SET payload = payload - 'scheduler_enabled'
 || jsonb_build_object('is_preemptible', payload->'scheduler_enabled')
WHERE payload ? 'scheduler_enabled'
"""
    )
    op.execute(
        """\
UPDATE jobs
SET payload = payload - 'preemptible_node'
 || jsonb_build_object('is_preemptible_node_required', payload->'preemptible_node')
WHERE payload ? 'preemptible_node'
"""
    )
