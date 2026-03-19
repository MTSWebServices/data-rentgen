# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add job_dependency table

Revision ID: 4e119cb7481e
Revises: a1950f06a8cb
Create Date: 2026-03-06 17:39:31.296534

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4e119cb7481e"
down_revision = "a1950f06a8cb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_dependency",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("from_job_id", sa.BigInteger(), nullable=False),
        sa.Column("to_job_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["from_job_id"],
            ["job.id"],
            name=op.f("fk__job_dependency__from_job_id__job"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["to_job_id"],
            ["job.id"],
            name=op.f("fk__job_dependency__to_job_id__job"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk__job_dependency")),
        sa.UniqueConstraint("from_job_id", "to_job_id", name=op.f("uq__job_dependency__from_job_id_to_job_id")),
    )
    op.create_index(op.f("ix__job_dependency__from_job_id"), "job_dependency", ["from_job_id"], unique=False)
    op.create_index(op.f("ix__job_dependency__to_job_id"), "job_dependency", ["to_job_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix__job_dependency__to_job_id"), table_name="job_dependency")
    op.drop_index(op.f("ix__job_dependency__from_job_id"), table_name="job_dependency")
    op.drop_table("job_dependency")
