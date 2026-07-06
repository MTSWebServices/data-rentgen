# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Detach Spark unknown jobs from everything

It should have no parent job, no dependencies and no tags.

Revision ID: 96fd9a096682
Revises: 4a02d2d5c8b1
Create Date: 2026-07-06 13:01:46.138108

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "96fd9a096682"
down_revision = "4a02d2d5c8b1"
branch_labels = None
depends_on = None

SPARK_UNKNOWN_JOBS_QUERY = """
    SELECT job.*
    FROM job
    JOIN job_type ON job.type_id = job_type.id
    WHERE job_type.type = 'SPARK_APPLICATION'
    AND lower(job.name) = 'unknown'
"""


def upgrade() -> None:
    op.execute(
        sa.text(
            f"""
            WITH data AS ({SPARK_UNKNOWN_JOBS_QUERY})
            UPDATE job
            SET parent_job_id=NULL
            WHERE id IN (SELECT id FROM data);
            """,
        ),
    )
    op.execute(
        sa.text(
            f"""
            WITH data AS ({SPARK_UNKNOWN_JOBS_QUERY})
            DELETE FROM job_dependency
            WHERE from_job_id IN (SELECT id FROM data) OR to_job_id IN (SELECT id FROM data);
            """,
        ),
    )
    op.execute(
        sa.text(
            f"""
            WITH data AS ({SPARK_UNKNOWN_JOBS_QUERY})
            DELETE FROM job_tag_value
            WHERE job_id IN (SELECT id FROM data);
            """,
        ),
    )


def downgrade() -> None:
    # this migration is irreversible
    pass
