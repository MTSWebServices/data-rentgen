# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add job_tag_value

Revision ID: 675ed36b7807
Revises: f1c76c422433
Create Date: 2026-01-21 11:30:01.406676

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "675ed36b7807"
down_revision = "f1c76c422433"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_tag_value",
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("tag_value_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name=op.f("fk__job_tag_value__job_id__job"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tag_value_id"],
            ["tag_value.id"],
            name=op.f("fk__job_tag_value__tag_value_id__tag_value"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("job_id", "tag_value_id", name=op.f("pk__job_tag_value")),
    )
    op.create_index("ix__job_tag_value__tag_value_id", "job_tag_value", ["tag_value_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix__job_tag_value__tag_value_id", table_name="job_tag_value")
    op.drop_table("job_tag_value")
