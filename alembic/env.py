import sys
from collections.abc import Iterable

from neuro_logging import init_logging
from sqlalchemy import engine_from_config, pool

from alembic import context
from alembic.operations import MigrationScript
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from platform_api.config_factory import EnvironConfigFactory, to_sync_postgres_dsn
from platform_api.orchestrator.jobs_storage.postgres import JobTables

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if sys.argv[0].endswith("alembic"):
    init_logging()

JobTables.create()  # populate metadata
target_metadata = JobTables.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario, we need to create an Engine
    and associate a connection with the context.

    """

    def process_revision_directives(
        context: MigrationContext,
        revision: str | Iterable[str | None] | Iterable[str],
        directives: list[MigrationScript],
    ) -> None:
        migration_script = directives[0]
        head_revision = ScriptDirectory.from_config(context.config).get_current_head()  # type: ignore[arg-type]
        if head_revision is None:
            new_rev_id = 1
        else:
            last_rev_id = int(head_revision.lstrip("0"))
            new_rev_id = last_rev_id + 1

        # fill zeros
        migration_script.rev_id = "{0:04}".format(new_rev_id)  # noqa UP032,UP030

    if not config.get_main_option("sqlalchemy.url"):
        db_config = EnvironConfigFactory().create_postgres()
        postgres_dsn = to_sync_postgres_dsn(db_config.postgres_dsn)
        config.set_main_option("sqlalchemy.url", postgres_dsn)

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=process_revision_directives,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
