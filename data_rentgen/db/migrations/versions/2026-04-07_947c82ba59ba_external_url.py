# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Add external_url and external_id for datasets

Revision ID: 947c82ba59ba
Revises: 4e119cb7481e
Create Date: 2026-04-07 14:16:26.411705

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "947c82ba59ba"
down_revision = "4e119cb7481e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dataset", sa.Column("external_id", sa.String(), nullable=True))
    op.add_column("dataset", sa.Column("external_url", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("dataset", "external_url")
    op.drop_column("dataset", "external_id")
