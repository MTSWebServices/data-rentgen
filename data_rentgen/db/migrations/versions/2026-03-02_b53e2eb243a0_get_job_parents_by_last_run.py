# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Get jobs parents by last run

Revision ID: b53e2eb243a0
Revises: a1950f06a8cb
Create Date: 2026-03-02 17:51:53.747020

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b53e2eb243a0"
down_revision = "a1950f06a8cb"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
    op.execute(
        """
        UPDATE job j
        SET parent_job_id = NULL
        """
    )
