# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Change dataset_column_relation primary key

Revision ID: 7d9f6a1c2b34
Revises: 65a6e6f1f11a
Create Date: 2026-07-14 15:51:48.132317

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7d9f6a1c2b34"
down_revision = "65a6e6f1f11a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE dataset_column_relation SET target_column = '' WHERE target_column IS NULL")
    op.alter_column(
        "dataset_column_relation",
        "target_column",
        existing_type=sa.String(length=255),
        nullable=False,
    )
    op.drop_constraint(op.f("pk__dataset_column_relation"), "dataset_column_relation", type_="primary")
    op.drop_column("dataset_column_relation", "id")
    op.create_primary_key(
        op.f("pk__dataset_column_relation"),
        "dataset_column_relation",
        ["fingerprint", "source_column", "target_column"],
    )
    op.drop_index(
        op.f("ix__dataset_column_relation__fingerprint_source_column_target_column"),
        table_name="dataset_column_relation",
    )
    op.execute("DROP SEQUENCE IF EXISTS dataset_column_relation_id_seq")


def downgrade() -> None:
    op.drop_constraint(op.f("pk__dataset_column_relation"), "dataset_column_relation", type_="primary")
    op.execute("CREATE SEQUENCE IF NOT EXISTS dataset_column_relation_id_seq")
    op.add_column("dataset_column_relation", sa.Column("id", sa.BigInteger(), nullable=True))
    op.execute("ALTER SEQUENCE dataset_column_relation_id_seq OWNED BY dataset_column_relation.id")
    op.execute("UPDATE dataset_column_relation SET id = nextval('dataset_column_relation_id_seq')")
    op.alter_column(
        "dataset_column_relation",
        "id",
        existing_type=sa.BigInteger(),
        nullable=False,
        server_default=sa.text("nextval('dataset_column_relation_id_seq')"),
    )
    op.create_primary_key(op.f("pk__dataset_column_relation"), "dataset_column_relation", ["id"])
    op.create_index(
        op.f("ix__dataset_column_relation__fingerprint_source_column_target_column"),
        "dataset_column_relation",
        ["fingerprint", "source_column", sa.text("coalesce(target_column, '')")],
        unique=True,
    )
    op.alter_column(
        "dataset_column_relation",
        "target_column",
        existing_type=sa.String(length=255),
        nullable=True,
    )
    op.execute("UPDATE dataset_column_relation SET target_column = NULL WHERE target_column = ''")
