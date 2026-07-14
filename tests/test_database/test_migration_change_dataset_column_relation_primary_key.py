# SPDX-FileCopyrightText: 2025-present MTS PJSC
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

PREV_REVISION = "65a6e6f1f11a"
THIS_REVISION = "7d9f6a1c2b34"


def test_migration_change_dataset_column_relation_primary_key(
    empty_db_url: str,
    alembic_config: AlembicConfig,
):
    do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

    engine = create_engine(empty_db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO dataset_column_relation
                        (id, fingerprint, source_column, target_column, type)
                    VALUES
                        (7, '00000000-0000-0000-0000-000000000001', 'source1', 'target1', 2),
                        (42, '00000000-0000-0000-0000-000000000001', 'source2', null, 128)
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.begin() as conn:
            assert conn.execute(
                text(
                    """
                    SELECT fingerprint::text, source_column, target_column, type
                    FROM dataset_column_relation
                    ORDER BY source_column
                    """,
                ),
            ).fetchall() == [
                ("00000000-0000-0000-0000-000000000001", "source1", "target1", 2),
                ("00000000-0000-0000-0000-000000000001", "source2", "", 128),
            ]
            conn.execute(
                text(
                    """
                    INSERT INTO dataset_column_relation
                        (fingerprint, source_column, target_column, type)
                    VALUES ('00000000-0000-0000-0000-000000000002', 'source3', '', 64)
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, PREV_REVISION, "down")

        with engine.begin() as conn:
            assert conn.execute(
                text(
                    """
                    SELECT fingerprint::text, source_column, target_column, type
                    FROM dataset_column_relation
                    ORDER BY source_column
                    """,
                ),
            ).fetchall() == [
                ("00000000-0000-0000-0000-000000000001", "source1", "target1", 2),
                ("00000000-0000-0000-0000-000000000001", "source2", None, 128),
                ("00000000-0000-0000-0000-000000000002", "source3", None, 64),
            ]

            inserted_id = conn.execute(
                text(
                    """
                    INSERT INTO dataset_column_relation
                        (fingerprint, source_column, target_column, type)
                    VALUES ('00000000-0000-0000-0000-000000000003', 'source4', null, 512)
                    RETURNING id
                    """,
                ),
            ).scalar_one()
            assert inserted_id is not None
    finally:
        engine.dispose()
