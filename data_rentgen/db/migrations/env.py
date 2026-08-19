# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from data_rentgen.db.models import Base
from data_rentgen.db.settings import DatabaseApplicationSettings

config = context.config


if config.config_file_name is not None:
    fileConfig(config.config_file_name)

if not config.get_main_option("sqlalchemy.url"):
    # read application settings only if sqlalchemy.url is not being passed via cli arguments
    config.set_main_option("sqlalchemy.url", str(DatabaseApplicationSettings().database.url).replace("%", "%%"))

target_metadata = (Base.metadata,)

PARTITION_PREFIXES = ["run_y", "operation_y", "input_y", "output_y", "column_lineage_y"]


def include_all_except_partitions(object_, name, type_, reflected, compare_to):
    if type_ == "table":
        for prefix in PARTITION_PREFIXES:
            if name.startswith(prefix):
                return False

    return True


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
        target_metadata=target_metadata,  # type: ignore[arg-type]
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_all_except_partitions,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=include_all_except_partitions,
    )  # type: ignore[arg-type]

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
