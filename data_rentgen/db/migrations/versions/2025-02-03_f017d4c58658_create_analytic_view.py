# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""create_analytics_views

Revision ID: f017d4c58658
Revises: 56f5a3f9442a
Create Date: 2025-02-03 16:44:27.746785

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f017d4c58658"
down_revision = "56f5a3f9442a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for base_table in ("output", "input"):
        for depth, suffix in view_sufix_map.items():
            view_name = base_table + suffix
            op.execute(sa.text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}"))
            op.execute(sa.text(get_statement(base_table, depth, suffix)))


def downgrade() -> None:
    for base_table in ("output", "input"):
        for suffix in view_sufix_map.values():
            view_name = base_table + suffix
            op.execute(sa.text(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}"))


view_sufix_map = {
    "day": "_daily_stats",
    "week": "_weekly_stats",
    "month": "_monthly_stats",
}


def get_statement(base_table: str, depth: str, suffix: str) -> str:
    view_name = base_table + suffix
    return f"""
        CREATE MATERIALIZED VIEW {view_name}
        AS (
            WITH aggregates AS (
            SELECT
                {base_table}.dataset_id as dataset_id
                , u.id as user_id
                , u.name as user_name
                , max({base_table}.created_at) as last_interaction_dt
                , count(*) as num_of_interactions
                , sum(num_bytes) as sum_bytes
                , sum(num_rows) as sum_rows
                , sum(num_files) as sum_files
                FROM {base_table}
                JOIN run r ON {base_table}.run_id = r.id
                JOIN "user" u ON r.started_by_user_id = u.id
                WHERE {base_table}.created_at >= now() - interval '1 {depth}'
                GROUP BY {base_table}.dataset_id, u.id
            )
            SELECT
                d.name as dataset_name
                , l.name as dataset_location
                , l.type as dataset_location_type
                , agr.user_id
                , agr.user_name
                , agr.last_interaction_dt
                , agr.num_of_interactions
                , agr.sum_bytes
                , agr.sum_rows
                , agr.sum_files
            FROM aggregates agr
            JOIN dataset d ON agr.dataset_id = d.id
            JOIN location l ON d.location_id = l.id
        )
        WITH NO DATA
    """
