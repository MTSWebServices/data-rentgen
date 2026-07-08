# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Delete datasets with reserved names

Revision ID: 9579bc0e1b35
Revises: 96fd9a096682
Create Date: 2026-07-08 11:52:15.440901

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9579bc0e1b35"
down_revision = "96fd9a096682"
branch_labels = None
depends_on = None

OPTIONAL_DATABASE_PATTERN = r"([\w\d_\-\.]+\.)?"
RESERVED_DATASET_NAME_PATTERNS = [
    r"information_schema\.[\w_.]+",  # common for all databases
    r"pg_[\w_.]+",  # PostgreSQL
    r"system\.[\w_.]+",  # Clickhouse
    r"sys\.[\w_.]+",  # Oracle
    "dual",
    r"all_[\w_]+",
    r"user_[\w_]+",
    r"dba_[\w_]+",
    r"v\$[\w_]+",
    r"v_\$[\w_]+",
    r"gv_\$[\w_]+",
]
RESERVED_DATASET_NAME_PATTERN = "^" + OPTIONAL_DATABASE_PATTERN + "(" + "|".join(RESERVED_DATASET_NAME_PATTERNS) + ")$"


def upgrade() -> None:
    op.execute(sa.text("CREATE TEMP TABLE datasets_to_delete (id BIGINT)"))

    op.execute(
        sa.text(
            """
            INSERT INTO datasets_to_delete (id)
            SELECT dataset.id
            FROM dataset
            WHERE name ~* :pattern
            """,
        ).bindparams(sa.bindparam("pattern", value=RESERVED_DATASET_NAME_PATTERN, type_=sa.String())),
    )

    op.execute(
        sa.text(
            """
            DELETE FROM input
            WHERE dataset_id IN (SELECT id FROM datasets_to_delete)
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM output
            WHERE dataset_id IN (SELECT id FROM datasets_to_delete)
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM column_lineage
            WHERE source_dataset_id IN (SELECT id FROM datasets_to_delete)
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM column_lineage
            WHERE target_dataset_id IN (SELECT id FROM datasets_to_delete)
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM dataset_symlink_group
            WHERE fingerprint IN (
                SELECT fingerprint
                FROM dataset_symlink_group
                WHERE dataset_id IN (SELECT id FROM datasets_to_delete)
            )
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM dataset
            WHERE id IN (SELECT id FROM datasets_to_delete)
            """,
        ),
    )


def downgrade() -> None:
    # this migration is irreversible
    pass
