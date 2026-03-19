# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Rename table dataset_tags to dataset_tag

Revision ID: f1c76c422433
Revises: 8e8891273099
Create Date: 2026-01-21 11:27:20.255770

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f1c76c422433"
down_revision = "8e8891273099"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("dataset_tags", "dataset_tag_value")
    op.execute("ALTER TABLE dataset_tag_value RENAME CONSTRAINT pk__dataset_tags TO pk__dataset_tag_value")
    op.execute(
        "ALTER TABLE dataset_tag_value "
        "RENAME CONSTRAINT fk__dataset_tags__tag_value_id__tag_value "
        "TO fk__dataset_tag_value__tag_value_id__tag_value"
    )
    op.execute(
        "ALTER TABLE dataset_tag_value "
        "RENAME CONSTRAINT fk__dataset_tags__dataset_id__dataset "
        "TO fk__dataset_tag_value__dataset_id__dataset"
    )
    op.execute("ALTER INDEX ix__dataset_tags__tag_value_id RENAME TO ix__dataset_tag_value__tag_value_id")


def downgrade() -> None:
    op.rename_table("dataset_tag_value", "dataset_tags")
    op.execute("ALTER TABLE dataset_tags RENAME CONSTRAINT pk__dataset_tag_value TO pk__dataset_tags")
    op.execute(
        "ALTER TABLE dataset_tags "
        "RENAME CONSTRAINT fk__dataset_tag_value__tag_value_id__tag_value "
        "TO fk__dataset_tags__tag_value_id__tag_value"
    )
    op.execute(
        "ALTER TABLE dataset_tags "
        "RENAME CONSTRAINT fk__dataset_tag_value__dataset_id__dataset "
        "TO fk__dataset_tags__dataset_id__dataset"
    )
    op.execute("ALTER INDEX ix__dataset_tag_value__tag_value_id RENAME TO ix__dataset_tags__tag_value_id")
