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

PREV_REVISION = "bff610745bac"
THIS_REVISION = "65a6e6f1f11a"


def test_migration_change_job_dependency_primary_key(empty_db_url: str, alembic_config: AlembicConfig):
    do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

    engine = create_engine(empty_db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO location (id, type, name) VALUES (1, 'local', 'somehost')"))
            conn.execute(
                text(
                    """
                    INSERT INTO job (id, location_id, name, type_id, parent_job_id) VALUES
                        (1, 1, 'task1', 3, null),
                        (2, 1, 'task2', 3, null),
                        (3, 1, 'task3', 3, null)
                    """,
                ),
            )
            conn.execute(
                text(
                    """
                    INSERT INTO job_dependency (id, from_job_id, to_job_id, type) VALUES
                        (7, 1, 2, 'DIRECT_DEPENDENCY'),
                        (42, 2, 3, null)
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT from_job_id, to_job_id, type FROM job_dependency ORDER BY from_job_id, to_job_id"),
            ).fetchall() == [
                (1, 2, "DIRECT_DEPENDENCY"),
                (2, 3, None),
            ]

        do_run_migrations(alembic_config, Base.metadata, PREV_REVISION, "down")

        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT id, from_job_id, to_job_id, type FROM job_dependency ORDER BY from_job_id, to_job_id"),
            ).fetchall()
            assert [(row.from_job_id, row.to_job_id, row.type) for row in rows] == [
                (1, 2, "DIRECT_DEPENDENCY"),
                (2, 3, None),
            ]

            conn.execute(
                text(
                    """
                    INSERT INTO job (id, location_id, name, type_id, parent_job_id)
                    VALUES (4, 1, 'task4', 3, null)
                    """,
                ),
            )
            inserted_id = conn.execute(
                text(
                    """
                    INSERT INTO job_dependency (from_job_id, to_job_id, type)
                    VALUES (3, 4, 'DIRECT_DEPENDENCY')
                    RETURNING id
                    """,
                ),
            ).scalar_one()
            assert inserted_id is not None

    finally:
        engine.dispose()
