# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Fix ClickHouse/MySQL dataset names

OpenLineage <1.47 incorrectly included the JDBC default database
for ClickHouse and MySQL, producing 3-component names like
``default.mydb.mytable`` instead of ``mydb.mytable``.

https://github.com/OpenLineage/OpenLineage/issues/4494
https://github.com/OpenLineage/OpenLineage/issues/4496
https://github.com/OpenLineage/OpenLineage/issues/4497

Revision ID: c3f8a2e1d749
Revises: 947c82ba59ba
Create Date: 2026-06-15 18:28:24.545123

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3f8a2e1d749"
down_revision = "947c82ba59ba"
branch_labels = None
depends_on = None

SCHEMALESS_TYPES = ["clickhouse", "mysql"]


def upgrade() -> None:
    # Create a map old 3-component dataset id -> new 2-component dataset id
    op.execute(sa.text("CREATE TEMP TABLE dataset_name_migration (old_id BIGINT, new_id BIGINT)"))

    # Ensure 2-component dataset exists for each 3-component one
    # The unique index ix__dataset__location_id__name_lower prevents duplicates
    op.execute(
        sa.text(
            """
            INSERT INTO dataset (name, location_id)
            SELECT DISTINCT
                substring(d.name FROM position('.' IN d.name) + 1),
                d.location_id
            FROM dataset d
            JOIN location l ON l.id = d.location_id
            WHERE l.type = ANY(:types)
              AND array_length(string_to_array(d.name, '.'), 1) = 3
            ON CONFLICT DO NOTHING
            """,
        ).bindparams(sa.bindparam("types", value=SCHEMALESS_TYPES, type_=sa.ARRAY(sa.String()))),
    )

    # Fill mapping table
    op.execute(
        sa.text(
            """
            INSERT INTO dataset_name_migration (old_id, new_id)
            SELECT d_old.id, d_new.id
            FROM dataset d_old
            JOIN location l ON l.id = d_old.location_id
            JOIN dataset d_new
                ON d_new.location_id = d_old.location_id
               AND lower(d_new.name) = lower(substring(d_old.name FROM position('.' IN d_old.name) + 1))
               AND d_new.id <> d_old.id
            WHERE l.type = ANY(:types)
              AND array_length(string_to_array(d_old.name, '.'), 1) = 3
            """,
        ).bindparams(sa.bindparam("types", value=SCHEMALESS_TYPES, type_=sa.ARRAY(sa.String()))),
    )

    # Remap FK references to old datasets onto new datasets
    op.execute(
        sa.text(
            """
            UPDATE input i
            SET dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE i.dataset_id = dm.old_id
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE output o
            SET dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE o.dataset_id = dm.old_id
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE column_lineage cl
            SET source_dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE cl.source_dataset_id = dm.old_id
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE column_lineage cl
            SET target_dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE cl.target_dataset_id = dm.old_id
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE dataset_symlink ds
            SET from_dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE ds.from_dataset_id = dm.old_id
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE dataset_symlink ds
            SET to_dataset_id = dm.new_id
            FROM dataset_name_migration dm
            WHERE ds.to_dataset_id = dm.old_id
            """,
        ),
    )
    # dataset_tag_value has a composite PK (dataset_id, tag_value_id);
    # insert new rows first, then delete the old ones.
    op.execute(
        sa.text(
            """
            INSERT INTO dataset_tag_value (dataset_id, tag_value_id)
            SELECT dm.new_id, dtv.tag_value_id
            FROM dataset_tag_value dtv
            JOIN dataset_name_migration dm ON dm.old_id = dtv.dataset_id
            ON CONFLICT DO NOTHING
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM dataset_tag_value
            WHERE dataset_id IN (SELECT old_id FROM dataset_name_migration)
            """,
        ),
    )

    # Delete old 3-component datasets
    # Since we remapped all references, ON DELETE CASCADE won't hurt
    op.execute(
        sa.text(
            """
            DELETE FROM dataset
            WHERE id IN (SELECT old_id FROM dataset_name_migration)
            """,
        ),
    )


def downgrade() -> None:
    # This migration is irreversible
    pass
