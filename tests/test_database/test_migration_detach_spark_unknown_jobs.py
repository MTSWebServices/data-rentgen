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

PREV_REVISION = "4a02d2d5c8b1"
THIS_REVISION = "96fd9a096682"


def test_migration_detach_spark_unknown_jobs(empty_db_url: str, alembic_config: AlembicConfig):
    do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

    engine = create_engine(empty_db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO location (id, type, name) VALUES
                        (1, 'local', 'somehost');
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO job (id, location_id, name, type_id, parent_job_id) VALUES
                        (1, 1, 'airflow_task1', 3, null),
                        (2, 1, 'airflow_task2', 3, null),
                        (3, 1, 'unknown', 1, 1),
                        (4, 1, 'some_session', 1, 2);
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO tag (id, name) VALUES
                        (1, 'some_tag'),
                        (2, 'another_tag');
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO tag_value (id, tag_id, value) VALUES
                        (1, 1, 'some_value'),
                        (2, 2, 'another_value');
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO job_tag_value (job_id, tag_value_id) VALUES
                        (1, 1),
                        (2, 1),
                        (3, 2),
                        (4, 2);
                    """,
                ),
            )

            conn.execute(
                text(
                    """
                    INSERT INTO job_dependency (from_job_id, to_job_id) VALUES
                        (1, 2),
                        (3, 4);
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT id, parent_job_id FROM job ORDER BY id"),
            ).fetchall() == [
                (1, None),
                (2, None),
                (3, None),  # parent_id is null
                (4, 2),
            ]

            assert conn.execute(
                text("SELECT job_id, tag_value_id FROM job_tag_value ORDER BY job_id, tag_value_id"),
            ).fetchall() == [
                (1, 1),
                (2, 1),
                # no item (3, 2)
                (4, 2),
            ]

            assert conn.execute(
                text("SELECT from_job_id, to_job_id FROM job_dependency ORDER BY from_job_id, to_job_id"),
            ).fetchall() == [
                (1, 2),
                # no item (3, 4)
            ]

    finally:
        engine.dispose()
