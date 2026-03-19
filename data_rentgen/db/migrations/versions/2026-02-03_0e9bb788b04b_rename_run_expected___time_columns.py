# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Rename run.expected_%_time columns

Revision ID: 0e9bb788b04b
Revises: b6a12e72a04e
Create Date: 2026-02-03 11:10:05.902453

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0e9bb788b04b"
down_revision = "b6a12e72a04e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("ALTER TABLE run RENAME COLUMN expected_start_time TO expected_start_at"))
    op.execute(sa.text("ALTER TABLE run RENAME COLUMN expected_end_time TO expected_end_at"))


def downgrade() -> None:
    op.execute(sa.text("ALTER TABLE run RENAME COLUMN expected_start_at TO expected_start_time"))
    op.execute(sa.text("ALTER TABLE run RENAME COLUMN expected_end_at TO expected_end_time"))
