"""create jobs table

Revision ID: eaa33ba10d63
Revises: 
Create Date: 2020-08-17 12:04:17.252280

"""
from alembic import op
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg


# revision identifiers, used by Alembic.
revision = 'eaa33ba10d63'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("cluster_name", sa.String(), nullable=False),
        sa.Column("is_preemptible", sa.Boolean(), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), nullable=True),
        sa.Column("max_run_time_minutes", sa.String(), nullable=True),
        sa.Column("internal_hostname", sa.String(), nullable=True),
        sa.Column("internal_hostname_named", sa.String(), nullable=True),
        sa.Column("schedule_timeout", sa.Float(), nullable=True),
        sa.Column("restart_policy", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False),
        sa.Column("finished_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=True),
        sa.Column("request", sapg.JSONB(), nullable=False),
        sa.Column("statuses", sapg.JSONB(), nullable=False),
        sa.Column("tags", sapg.JSONB(), nullable=True),
    )
    # Index to simulate conditional unique constraint
    op.create_index('jobs_name_owner_uq', 'jobs', ['name', 'owner'], unique=True,
                    postgresql_where=sa.text("(jobs.status != 'succeeded' AND jobs.status != 'failed')"))
    op.execute("""\
create or replace function sort_json_str_array(jsonb)
returns jsonb language sql as $$
    select jsonb_agg(value order by value)
    from jsonb_array_elements_text($1)
$$;
create or replace function enumerate_json_array(jsonb)
returns jsonb language sql as $$
    select jsonb_agg(t order by index)
    from jsonb_array_elements($1) with ordinality as t(value, index)
$$;
""")


def downgrade() -> None:
    op.drop_table("jobs")