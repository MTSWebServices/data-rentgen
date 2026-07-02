# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from data_rentgen.db.models import Base
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    compute_symlink_fingerprint,
)
from tests.test_database.fixtures.alembic import do_run_migrations

if TYPE_CHECKING:
    from alembic.config import Config as AlembicConfig

pytestmark = [pytest.mark.db]

PREV_REVISION = "c3f8a2e1d749"
THIS_REVISION = "4a02d2d5c8b1"


def _dataset(location_type: str, location_name: str, name: str) -> DatasetDTO:
    return DatasetDTO(
        name=name,
        location=LocationDTO(type=location_type, name=location_name, addresses=set()),
    )


def test_migration_backfill_dataset_symlink_group(empty_db_url: str, alembic_config: AlembicConfig):
    do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

    engine = create_engine(empty_db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO location (id, type, name) VALUES
                        (1, 'hive', 'metastore'),
                        (2, 'hdfs', 'cluster')
                    """,
                ),
            )
            conn.execute(
                text(
                    """
                    INSERT INTO dataset (id, location_id, name) VALUES
                        (10, 1, 'schema.table1'),
                        (11, 1, 'schema.table2'),
                        (20, 2, '/warehouse/table')
                    """,
                ),
            )
            conn.execute(
                text(
                    """
                    INSERT INTO dataset_symlink (id, from_dataset_id, to_dataset_id, type) VALUES
                        (1, 20, 10, 'METASTORE'),
                        (2, 10, 20, 'WAREHOUSE'),
                        (3, 20, 11, 'METASTORE'),
                        (4, 11, 20, 'WAREHOUSE')
                    """,
                ),
            )

        do_run_migrations(alembic_config, Base.metadata, THIS_REVISION)

        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT fingerprint::text, dataset_id, type FROM dataset_symlink_group"),
            ).fetchall()

        hdfs = _dataset("hdfs", "cluster", "/warehouse/table")
        hive1 = _dataset("hive", "metastore", "schema.table1")
        hive2 = _dataset("hive", "metastore", "schema.table2")

        fingerprint_a = compute_symlink_fingerprint(
            [(hdfs, DatasetSymlinkTypeDTO.WAREHOUSE), (hive1, DatasetSymlinkTypeDTO.METASTORE)],
        )
        fingerprint_b = compute_symlink_fingerprint(
            [(hdfs, DatasetSymlinkTypeDTO.WAREHOUSE), (hive2, DatasetSymlinkTypeDTO.METASTORE)],
        )

        actual = {(fingerprint, dataset_id, type_) for fingerprint, dataset_id, type_ in rows}
        assert actual == {
            (str(fingerprint_a), 20, "WAREHOUSE"),
            (str(fingerprint_a), 10, "METASTORE"),
            (str(fingerprint_b), 20, "WAREHOUSE"),
            (str(fingerprint_b), 11, "METASTORE"),
        }

        assert fingerprint_a != fingerprint_b
        fingerprints_of_hdfs = {fingerprint for fingerprint, dataset_id, _ in rows if dataset_id == 20}
        assert fingerprints_of_hdfs == {str(fingerprint_a), str(fingerprint_b)}

        # The old dataset_symlink table is replaced by a VIEW
        with engine.connect() as conn:
            view_rows = conn.execute(
                text(
                    "SELECT from_dataset_id, to_dataset_id, type FROM dataset_symlink ORDER BY from_dataset_id, to_dataset_id"
                ),
            ).fetchall()

        assert set(view_rows) == {
            (10, 20, "WAREHOUSE"),
            (20, 10, "METASTORE"),
            (11, 20, "WAREHOUSE"),
            (20, 11, "METASTORE"),
        }

        do_run_migrations(alembic_config, Base.metadata, PREV_REVISION)

        with engine.connect() as conn:
            downgraded_rows = conn.execute(
                text(
                    "SELECT from_dataset_id, to_dataset_id, type FROM dataset_symlink ORDER BY from_dataset_id, to_dataset_id"
                ),
            ).fetchall()

        assert set(downgraded_rows) == {
            (10, 20, "WAREHOUSE"),
            (20, 10, "METASTORE"),
            (11, 20, "WAREHOUSE"),
            (20, 11, "METASTORE"),
        }

    finally:
        engine.dispose()
