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

PREV_REVISION = "96fd9a096682"
THIS_REVISION = "9579bc0e1b35"


def test_migration_delete_datasets_with_reserved_names(empty_db_url: str, alembic_config: AlembicConfig):
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
                        (3, 'sqlserver', 'myhost:1433'),
                        (4, 'postgres', 'myhost:5432'),
                        (5, 'oracle', 'myhost:1521')
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO dataset (location_id, name) VALUES
                        (1, 'default.mytable'),
                        (2, 'mydb.mytable'),
                        (3, 'mydb.myschema.mytable'),
                        (4, 'mydb.myschema.mytable'),
                        (5, 'mydb.myschema.mytable'),

                        (1, 'system.tables'),
                        (2, 'information_schema.tables'),

                        (3, 'mydb.information_schema.tables'),

                        (4, 'mydb.information_schema.tables'),
                        (4, 'mydb.pg_catalog.pg_database'),
                        (4, 'mydb.pg_database'),

                        (5, 'mydb.information_schema.tables'),
                        (5, 'mydb.sys.all_tables'),
                        (5, 'mydb.all_tables'),
                        (5, 'mydb.user_tables'),
                        (5, 'mydb.dba_tables'),
                        (5, 'mydb.dual'),
                        (5, 'mydb.v$session'),
                        (5, 'mydb.v_$session'),
                        (5, 'mydb.gv_$session')
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT location_id, name FROM dataset ORDER BY location_id, name"),
            ).fetchall() == [
                (1, "default.mytable"),
                (2, "mydb.mytable"),
                (3, "mydb.myschema.mytable"),
                (4, "mydb.myschema.mytable"),
                (5, "mydb.myschema.mytable"),
            ]

    finally:
        engine.dispose()
