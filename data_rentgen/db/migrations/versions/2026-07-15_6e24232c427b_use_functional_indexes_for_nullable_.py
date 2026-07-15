# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Use functional indexes for nullable columns

Revision ID: 6e24232c427b
Revises: 15f4778843f0
Create Date: 2026-07-15 12:40:59.066411

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "6e24232c427b"
down_revision = "15f4778843f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(op.f("ix__job__parent_job_id"), table_name="job")
    op.create_index(
        "ix__job__parent_job_id",
        "job",
        ["parent_job_id"],
        unique=False,
        postgresql_where="parent_job_id IS NOT NULL",
    )

    op.drop_index(op.f("ix__run__parent_run_id"), table_name="run")
    op.create_index(
        "ix__run__parent_run_id",
        "run",
        ["parent_run_id"],
        unique=False,
        postgresql_where="parent_run_id IS NOT NULL",
    )

    op.drop_index(op.f("ix__operation__sql_query_id"), table_name="operation")
    op.create_index(
        "ix__operation__sql_query_id",
        "operation",
        ["sql_query_id"],
        unique=False,
        postgresql_where="sql_query_id IS NOT NULL",
    )

    op.drop_index(op.f("ix__input__schema_id"), table_name="input")
    op.create_index(
        "ix__input__schema_id",
        "input",
        ["schema_id"],
        unique=False,
        postgresql_where="schema_id IS NOT NULL",
    )

    op.drop_index(op.f("ix__output__schema_id"), table_name="output")
    op.create_index(
        "ix__output__schema_id",
        "output",
        ["schema_id"],
        unique=False,
        postgresql_where="schema_id IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_index(op.f("ix__output__schema_id"), table_name="output")
    op.create_index(op.f("ix__output__schema_id"), "output", ["schema_id"], unique=False)

    op.drop_index(op.f("ix__input__schema_id"), table_name="input")
    op.create_index(op.f("ix__input__schema_id"), "input", ["schema_id"], unique=False)

    op.drop_index(op.f("ix__operation__sql_query_id"), table_name="operation")
    op.create_index(op.f("ix__operation__sql_query_id"), "operation", ["sql_query_id"], unique=False)

    op.drop_index(op.f("ix__run__parent_run_id"), table_name="run")
    op.create_index(op.f("ix__run__parent_run_id"), "run", ["parent_run_id"], unique=False)

    op.drop_index(op.f("ix__job__parent_job_id"), table_name="job")
    op.create_index(op.f("ix__job__parent_job_id"), "job", ["parent_job_id"], unique=False)
