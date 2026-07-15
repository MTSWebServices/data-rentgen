# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add index for run.started_at and run.ended_at

Revision ID: 484cee706cc2
Revises: 6e24232c427b
Create Date: 2026-07-15 13:28:23.888562

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "484cee706cc2"
down_revision = "6e24232c427b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix__run__ended_at",
        "run",
        ["ended_at"],
        unique=False,
        postgresql_where="ended_at IS NOT NULL",
        postgresql_using="brin",
    )
    op.create_index(
        "ix__run__started_at",
        "run",
        ["started_at"],
        unique=False,
        postgresql_where="started_at IS NOT NULL",
        postgresql_using="brin",
    )


def downgrade() -> None:
    op.drop_index("ix__run__started_at", table_name="run")
    op.drop_index("ix__run__ended_at", table_name="run")
