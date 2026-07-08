# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from data_rentgen.db.models import Base
from tests.test_database.fixtures.alembic import do_run_migrations

if TYPE_CHECKING:
    from alembic.config import Config as AlembicConfig

pytestmark = [pytest.mark.db]

PREV_REVISION = "947c82ba59ba"
THIS_REVISION = "c3f8a2e1d749"


def test_migration_fix_clickhouse_mysql_dataset_names(empty_db_url: str, alembic_config: AlembicConfig):
    do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

    engine = create_engine(empty_db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO location (id, type, name) VALUES
                        (1, 'clickhouse', 'myhost:8123'),
                        (2, 'mysql', 'myhost:3306'),
                        (3, 'postgres', 'myhost:5432')
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO dataset (location_id, name) VALUES
                        (1, 'mydb.collision'),
                        (1, 'default.mydb.collision'),
                        (1, 'default.mydb.mytable'),

                        (2, 'mydb.mydb.mytable'),
                        (3, 'mydb.myschema.mytable')
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT location_id, name FROM dataset ORDER BY location_id, name"),
            ).fetchall() == [
                (1, "mydb.collision"),
                (1, "mydb.mytable"),
                (2, "mydb.mytable"),
                (3, "mydb.myschema.mytable"),
            ]
    finally:
        engine.dispose()
