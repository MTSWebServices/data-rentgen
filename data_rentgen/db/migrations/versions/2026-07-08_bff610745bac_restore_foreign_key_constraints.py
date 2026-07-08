# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Restore foreign key constraints

Revision ID: bff610745bac
Revises: 9579bc0e1b35
Create Date: 2026-07-08 12:50:36.653743

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "bff610745bac"
down_revision = "9579bc0e1b35"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_foreign_key(op.f("fk__run__job_id__job"), "run", "job", ["job_id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key(op.f("fk__input__job_id__job"), "input", "job", ["job_id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key(op.f("fk__output__job_id__job"), "output", "job", ["job_id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key(
        op.f("fk__column_lineage__job_id__job"),
        "column_lineage",
        "job",
        ["job_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        op.f("fk__input__dataset_id__dataset"),
        "input",
        "dataset",
        ["dataset_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        op.f("fk__output__dataset_id__dataset"),
        "output",
        "dataset",
        ["dataset_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        op.f("fk__column_lineage__source_dataset_id__dataset"),
        "column_lineage",
        "dataset",
        ["source_dataset_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("fk__column_lineage__target_dataset_id__dataset"),
        "column_lineage",
        "dataset",
        ["target_dataset_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("fk__input__schema_id__schema"),
        "input",
        "schema",
        ["schema_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        op.f("fk__output__schema_id__schema"),
        "output",
        "schema",
        ["schema_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        op.f("fk__operation__sql_query_id__sql_query"),
        "operation",
        "sql_query",
        ["sql_query_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("fk__operation__sql_query_id__sql_query"), "output", type_="foreignkey")

    op.drop_constraint(op.f("fk__output__schema_id__schema"), "output", type_="foreignkey")
    op.drop_constraint(op.f("fk__input__schema_id__schema"), "input", type_="foreignkey")

    op.drop_constraint(op.f("fk__column_lineage__target_dataset_id__dataset"), "column_lineage", type_="foreignkey")
    op.drop_constraint(op.f("fk__column_lineage__source_dataset_id__dataset"), "column_lineage", type_="foreignkey")
    op.drop_constraint(op.f("fk__output__dataset_id__dataset"), "output", type_="foreignkey")
    op.drop_constraint(op.f("fk__input__dataset_id__dataset"), "input", type_="foreignkey")

    op.drop_constraint(op.f("fk__column_lineage__job_id__job"), "column_lineage", type_="foreignkey")
    op.drop_constraint(op.f("fk__output__job_id__job"), "output", type_="foreignkey")
    op.drop_constraint(op.f("fk__input__job_id__job"), "input", type_="foreignkey")
    op.drop_constraint(op.f("fk__run__job_id__job"), "run", type_="foreignkey")
