# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""Change job_dependency primary key

Revision ID: 65a6e6f1f11a
Revises: bff610745bac
Create Date: 2026-07-09 16:37:02.215531

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "65a6e6f1f11a"
down_revision = "bff610745bac"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(op.f("ix__job_dependency__from_job_id"), table_name="job_dependency")
    op.drop_constraint(op.f("pk__job_dependency"), "job_dependency", type_="primary")
    op.drop_constraint(op.f("uq__job_dependency__from_job_id_to_job_id"), "job_dependency", type_="unique")
    op.drop_column("job_dependency", "id")
    op.create_primary_key(op.f("pk__job_dependency"), "job_dependency", ["from_job_id", "to_job_id"])
    op.execute("DROP SEQUENCE IF EXISTS job_dependency_id_seq")


def downgrade() -> None:
    op.drop_constraint(op.f("pk__job_dependency"), "job_dependency", type_="primary")
    op.execute("CREATE SEQUENCE IF NOT EXISTS job_dependency_id_seq")
    op.add_column("job_dependency", sa.Column("id", sa.BigInteger(), nullable=True))
    op.execute("ALTER SEQUENCE job_dependency_id_seq OWNED BY job_dependency.id")
    op.execute("UPDATE job_dependency SET id = nextval('job_dependency_id_seq')")
    op.alter_column(
        "job_dependency",
        "id",
        existing_type=sa.BigInteger(),
        nullable=False,
        server_default=sa.text("nextval('job_dependency_id_seq')"),
    )
    op.create_primary_key(op.f("pk__job_dependency"), "job_dependency", ["id"])
    op.create_unique_constraint(
        op.f("uq__job_dependency__from_job_id_to_job_id"),
        "job_dependency",
        ["from_job_id", "to_job_id"],
    )
    op.create_index(op.f("ix__job_dependency__from_job_id"), "job_dependency", ["from_job_id"], unique=False)
