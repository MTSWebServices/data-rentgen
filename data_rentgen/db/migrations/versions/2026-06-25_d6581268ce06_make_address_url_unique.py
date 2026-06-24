# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Make address.url unique

Revision ID: d6581268ce06
Revises: c3f8a2e1d749
Create Date: 2026-06-25 00:48:14.782612

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d6581268ce06"
down_revision = "c3f8a2e1d749"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(op.f("uq__address__location_id_url"), "address", type_="unique")
    op.drop_index(op.f("ix__address__url"), table_name="address")
    op.create_index(op.f("ix__address__url"), "address", ["url"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix__address__url"), table_name="address")
    op.create_index(op.f("ix__address__url"), "address", ["url"], unique=False)
    op.create_unique_constraint(
        op.f("uq__address__location_id_url"),
        "address",
        ["location_id", "url"],
    )
