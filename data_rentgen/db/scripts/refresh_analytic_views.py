#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import sys
from argparse import ArgumentParser
from enum import Enum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.settings import DatabaseApplicationSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


class Depth(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    def __str__(self) -> str:
        return self.value


view_sufix_map = {
    "day": "_daily_stats",
    "week": "_weekly_stats",
    "month": "_monthly_stats",
}


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.create_analytics_view --depths day",
        description="Create matherialized views based on input and output table with given depths",
    )
    parser.add_argument(
        "--depths",
        type=Depth,
        choices=[item.value for item in Depth],
        nargs="+",
        help="Depth of matherialized view data (created_at filter). You can provide list of args",
    )
    return parser


async def refresh_view(depth: Depth, session: AsyncSession):
    for base_table in ("output", "input"):
        view_name = base_table + view_sufix_map[depth]
        logger.info("Refresh view: %s", view_name)
        statement = f"REFRESH MATERIALIZED VIEW {view_name}"
        await session.execute(text(statement))


async def main(args: list[str]) -> None:
    settings = DatabaseApplicationSettings()
    setup_logging(settings.logging)

    parser = get_parser()
    params = parser.parse_args(args)
    depths = params.depths
    if not depths:
        logger.info("Create views for all depths")
        depths = Depth
    else:
        depths = sorted(set(depths))

    session_factory = create_session_factory(settings.database)
    async with session_factory() as session:
        for depth in depths:
            await refresh_view(depth, session)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
