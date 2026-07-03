# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Create dataset_symlink_group

Revision ID: 4a02d2d5c8b1
Revises: c3f8a2e1d749
Create Date: 2026-06-29 19:40:00.000000

"""

import sqlalchemy as sa
from alembic import op

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    compute_symlink_fingerprint,
)

BACKFILL_BATCH_SIZE = 10_000

# revision identifiers, used by Alembic.
revision = "4a02d2d5c8b1"
down_revision = "c3f8a2e1d749"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_symlink_group",
        sa.Column("dataset_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("fingerprint", sa.UUID(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset_symlink_group__dataset_id__dataset"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "dataset_id",
            "fingerprint",
            name=op.f("pk__dataset_symlink_group"),
        ),
    )
    _backfill_symlink_groups()
    op.create_index(
        op.f("ix__dataset_symlink_group__fingerprint"),
        "dataset_symlink_group",
        ["fingerprint"],
    )
    op.drop_table("dataset_symlink")
    op.execute(
        sa.text(
            """
            CREATE VIEW dataset_symlink AS
            SELECT
                a.dataset_id AS from_dataset_id,
                b.dataset_id AS to_dataset_id,
                b.type       AS type
            FROM dataset_symlink_group a
            JOIN dataset_symlink_group b
              ON a.fingerprint = b.fingerprint
             AND a.dataset_id <> b.dataset_id
            """,
        ),
    )


def downgrade() -> None:
    op.execute(sa.text("ALTER VIEW dataset_symlink RENAME TO dataset_symlink_view"))
    op.create_table(
        "dataset_symlink",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("from_dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("to_dataset_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["from_dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset_symlink__from_dataset_id__dataset"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["to_dataset_id"],
            ["dataset.id"],
            name=op.f("fk__dataset_symlink__to_dataset_id__dataset"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__dataset_symlink")),
        sa.UniqueConstraint(
            "from_dataset_id",
            "to_dataset_id",
            name=op.f("uq__dataset_symlink__from_dataset_id_to_dataset_id"),
        ),
    )
    op.execute(
        sa.text(
            """
            INSERT INTO dataset_symlink (from_dataset_id, to_dataset_id, type)
            SELECT from_dataset_id, to_dataset_id, type
            FROM dataset_symlink_view
            ON CONFLICT (from_dataset_id, to_dataset_id) DO NOTHING
            """,
        ),
    )
    op.create_index(op.f("ix__dataset_symlink__from_dataset_id"), "dataset_symlink", ["from_dataset_id"])
    op.create_index(op.f("ix__dataset_symlink__to_dataset_id"), "dataset_symlink", ["to_dataset_id"])

    op.execute(sa.text("DROP VIEW dataset_symlink_view"))
    op.drop_table("dataset_symlink_group")


def _opposite_type(type_: DatasetSymlinkTypeDTO) -> DatasetSymlinkTypeDTO:
    if type_ == DatasetSymlinkTypeDTO.METASTORE:
        return DatasetSymlinkTypeDTO.WAREHOUSE
    return DatasetSymlinkTypeDTO.METASTORE


def _backfill_symlink_groups() -> None:
    bind = op.get_bind()
    last_id = 0

    query = sa.text(
        """
        SELECT
            ds.id AS id,
            ds.from_dataset_id AS from_dataset_id,
            ds.to_dataset_id AS to_dataset_id,
            ds.type AS type,
            from_location.type AS from_location_type,
            from_location.name AS from_location_name,
            from_dataset.name AS from_dataset_name,
            to_location.type AS to_location_type,
            to_location.name AS to_location_name,
            to_dataset.name AS to_dataset_name
        FROM dataset_symlink AS ds
        JOIN dataset AS from_dataset ON from_dataset.id = ds.from_dataset_id
        JOIN location AS from_location ON from_location.id = from_dataset.location_id
        JOIN dataset AS to_dataset ON to_dataset.id = ds.to_dataset_id
        JOIN location AS to_location ON to_location.id = to_dataset.location_id
        WHERE ds.id > :last_id
        ORDER BY ds.id
        LIMIT :limit
        """,
    )

    insert_query = sa.text(
        """
        INSERT INTO dataset_symlink_group (fingerprint, dataset_id, type)
        VALUES (:fingerprint, :dataset_id, :type)
        ON CONFLICT (dataset_id, fingerprint) DO NOTHING
        """,
    )

    while rows := bind.execute(query, {"last_id": last_id, "limit": BACKFILL_BATCH_SIZE}).fetchall():
        _insert_symlink_groups_batch(bind, insert_query, rows)
        last_id = rows[-1].id


def _insert_symlink_groups_batch(bind, insert_query, rows) -> None:
    params: list[dict] = []

    for row in rows:
        to_role = DatasetSymlinkTypeDTO(row.type)
        from_role = _opposite_type(to_role)

        from_dataset = DatasetDTO(
            name=row.from_dataset_name,
            location=LocationDTO(
                type=row.from_location_type,
                name=row.from_location_name,
                addresses=set(),
            ),
        )
        to_dataset = DatasetDTO(
            name=row.to_dataset_name,
            location=LocationDTO(
                type=row.to_location_type,
                name=row.to_location_name,
                addresses=set(),
            ),
        )

        fingerprint = compute_symlink_fingerprint(
            [(from_dataset, from_role), (to_dataset, to_role)],
        )

        params.append(
            {
                "fingerprint": fingerprint,
                "dataset_id": row.from_dataset_id,
                "type": str(from_role),
            },
        )
        params.append(
            {
                "fingerprint": fingerprint,
                "dataset_id": row.to_dataset_id,
                "type": str(to_role),
            },
        )

    if params:
        bind.execute(insert_query, params)
