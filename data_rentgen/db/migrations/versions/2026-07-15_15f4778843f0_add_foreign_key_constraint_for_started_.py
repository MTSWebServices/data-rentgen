# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add foreign key constraint for started_by_user_id

Revision ID: 15f4778843f0
Revises: 7d9f6a1c2b34
Create Date: 2026-07-15 12:31:44.913017

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "15f4778843f0"
down_revision = "7d9f6a1c2b34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix__run__started_by_user_id",
        "run",
        ["started_by_user_id"],
        unique=False,
        postgresql_where="started_by_user_id IS NOT NULL",
    )
    op.create_foreign_key(
        op.f("fk__run__started_by_user_id__user"),
        "run",
        "user",
        ["started_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("fk__run__started_by_user_id__user"), "run", type_="foreignkey")
    op.drop_index("ix__run__started_by_user_id", table_name="run", postgresql_where="started_by_user_id IS NOT NULL")
