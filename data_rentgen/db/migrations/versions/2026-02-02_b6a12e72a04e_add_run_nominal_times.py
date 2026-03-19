# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add start and end expected times for Run

Revision ID: b6a12e72a04e
Revises: 675ed36b7807
Create Date: 2026-02-02 12:49:00.686642

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b6a12e72a04e"
down_revision = "675ed36b7807"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("run", sa.Column("expected_start_time", sa.DateTime(timezone=True), nullable=True))
    op.add_column("run", sa.Column("expected_end_time", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("run", "expected_start_time")
    op.drop_column("run", "expected_end_time")
