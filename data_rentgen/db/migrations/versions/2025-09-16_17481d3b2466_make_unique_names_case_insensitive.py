# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Make unique names constraints case-insensitive

Revision ID: 17481d3b2466
Revises: fc001835e473
Create Date: 2025-09-16 11:18:01.308085

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "17481d3b2466"
down_revision = "fc001835e473"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("UPDATE location SET type = lower(type), name = lower(name)"))
    op.execute(sa.text("UPDATE address SET url = lower(url)"))
    op.execute(sa.text("UPDATE job_type SET type = upper(type)"))

    op.create_index(
        "ix__dataset__location_id__name_lower",
        "dataset",
        ["location_id", sa.literal_column("lower(name)")],
        unique=True,
    )
    op.drop_constraint(op.f("uq__dataset__location_id_name"), "dataset", type_="unique")
    op.drop_index(op.f("ix__dataset__location_id"), table_name="dataset")
    op.drop_index(op.f("ix__dataset__name"), table_name="dataset")

    op.create_index(
        "ix__job__location_id_name_lower",
        "job",
        ["location_id", sa.literal_column("lower(name)")],
        unique=True,
    )
    op.drop_constraint(op.f("uq__job__location_id_name"), "job", type_="unique")
    op.drop_index(op.f("ix__job__location_id"), table_name="job")
    op.drop_index(op.f("ix__job__name"), table_name="job")

    op.create_index("ix__tag__name_lower", "tag", [sa.literal_column("lower(name)")], unique=True)
    op.drop_constraint(op.f("uq__tag__name"), "tag", type_="unique")

    op.create_index(
        "ix__tag_value__tag_id_value_lower",
        "tag_value",
        ["tag_id", sa.literal_column("lower(value)")],
        unique=True,
    )
    op.drop_constraint(op.f("uq__tag_value__tag_id_value"), "tag_value", type_="unique")
    op.drop_index(op.f("ix__tag_value__tag_id"), table_name="tag_value")

    op.create_index("ix__user__name_lower", "user", [sa.literal_column("lower(name)")], unique=True)
    op.drop_index(op.f("ix__user__name"), table_name="user")


def downgrade() -> None:
    op.create_index(op.f("ix__user__name"), "user", ["name"], unique=True)
    op.drop_index("ix__user__name_lower", table_name="user")

    op.create_unique_constraint(
        op.f("uq__tag_value__tag_id_value"),
        "tag_value",
        ["tag_id", "value"],
    )
    op.create_index(op.f("ix__tag_value__tag_id"), "tag_value", ["tag_id"], unique=False)
    op.drop_index("ix__tag_value__tag_id_value_lower", table_name="tag_value")

    op.create_unique_constraint(op.f("uq__tag__name"), "tag", ["name"])
    op.drop_index("ix__tag__name_lower", table_name="tag")

    op.create_unique_constraint(op.f("uq__job_type__type"), "job_type", ["type"])
    op.create_index(op.f("ix__job_type__type"), "job_type", ["type"], unique=False)
    op.drop_index("ix__job_type__type_lower", table_name="job_type")

    op.create_index(op.f("ix__job__location_id"), "job", ["location_id"], unique=False)
    op.create_index(op.f("ix__job__name"), "job", ["name"], unique=False)
    op.create_unique_constraint(
        op.f("uq__job__location_id_name"),
        "job",
        ["location_id", "name"],
    )
    op.drop_index("ix__job__location_id_name_lower", table_name="job")

    op.create_unique_constraint(
        op.f("uq__dataset__location_id_name"),
        "dataset",
        ["location_id", "name"],
    )
    op.create_index(op.f("ix__dataset__name"), "dataset", ["name"], unique=False)
    op.create_index(op.f("ix__dataset__location_id"), "dataset", ["location_id"], unique=False)
    op.drop_index("ix__dataset__location_id__name_lower", table_name="dataset")
