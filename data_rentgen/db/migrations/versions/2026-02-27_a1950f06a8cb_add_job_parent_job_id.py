# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add parent_job_id for Job model

Revision ID: a1950f06a8cb
Revises: 0e9bb788b04b
Create Date: 2026-02-27 17:15:19.487309

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1950f06a8cb"
down_revision = "0e9bb788b04b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("job", sa.Column("parent_job_id", sa.BigInteger(), nullable=True))
    op.create_index(op.f("ix__job__parent_job_id"), "job", ["parent_job_id"], unique=False)
    op.create_foreign_key(op.f("fk__job__parent_job_id__job"), "job", "job", ["parent_job_id"], ["id"])

    op.execute(
        """
        UPDATE job j
        SET parent_job_id = parent_run.job_id
        FROM (
            SELECT DISTINCT ON (r.job_id)
                r.job_id,
                r.parent_run_id
            FROM run r
            ORDER BY r.job_id, r.created_at, r.id DESC
        ) last_run
        JOIN run parent_run ON parent_run.id = last_run.parent_run_id
        WHERE j.id = last_run.job_id
          AND last_run.parent_run_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_constraint(op.f("fk__job__parent_job_id__job"), "job", type_="foreignkey")
    op.drop_index(op.f("ix__job__parent_job_id"), table_name="job")
    op.drop_column("job", "parent_job_id")
