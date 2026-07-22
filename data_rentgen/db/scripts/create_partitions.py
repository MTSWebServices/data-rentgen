#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import sys
from argparse import ArgumentParser
from datetime import UTC, date, datetime
from enum import Enum

from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.models import Input, Operation, Output, Run
from data_rentgen.db.models.column_lineage import ColumnLineage
from data_rentgen.db.settings import DatabaseApplicationSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)

PARTITIONED_TABLES = [
    Run.__tablename__,
    Operation.__tablename__,
    Input.__tablename__,
    Output.__tablename__,
    ColumnLineage.__tablename__,
]


class Granularity(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"

    def __str__(self) -> str:
        return self.value

    def round(self, input_: date) -> date:
        if self == Granularity.DAY:
            return input_
        if self == Granularity.MONTH:
            return input_.replace(day=1)
        return input_.replace(day=1, month=1)

    def to_range(self) -> relativedelta:
        if self == Granularity.DAY:
            return relativedelta(days=1)
        if self == Granularity.MONTH:
            return relativedelta(months=1)
        return relativedelta(years=1)

    def to_format(self) -> str:
        if self == Granularity.DAY:
            return "y%Y_m%m_%d"
        if self == Granularity.MONTH:
            return "y%Y_m%m"
        return "y%Y"


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.create_partitions --start 2024-01 --granularity month",
        description="Create partitions for tables, for given start date and granularity.",
    )
    parser.add_argument(
        "--start",
        type=isoparse,
        default=datetime.now(tz=UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        nargs="?",
        help="Start date for partitions, default is the first day of current month.",
    )
    parser.add_argument(
        "--end",
        type=isoparse,
        default=datetime.now(tz=UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        + relativedelta(months=2),
        nargs="?",
        help="End date for partitions, default is the last day of next month.",
    )
    parser.add_argument(
        "--granularity",
        type=lambda x: Granularity(x).value,
        choices=[item.value for item in Granularity],
        default=Granularity.MONTH.value,
        nargs="?",
        help="Granularity of partitions, default is month.",
    )
    return parser


def generate_partition_range(start: date, end: date, granularity: Granularity) -> list[tuple[date, date]]:
    parts = []
    current = start
    next_part = current + granularity.to_range()
    while current < end:
        parts.append((current, next_part))
        current += granularity.to_range()
        next_part = current + granularity.to_range()
    if not parts:
        msg = "Data range is too small"
        raise ValueError(msg)

    return parts


async def create_partition(start: date, end: date, granularity: Granularity, session: AsyncSession):
    partition_name = start.strftime(granularity.to_format())

    start_str = start.isoformat()
    end_str = end.isoformat()

    for table in PARTITIONED_TABLES:
        statement = f"CREATE TABLE IF NOT EXISTS {table}_{partition_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')"  # noqa: E501
        logger.debug("Executing statement: %s", statement)
        await session.execute(text(statement))


async def main(args: list[str]) -> None:
    settings = DatabaseApplicationSettings()
    setup_logging(settings.logging)

    parser = get_parser()
    params = parser.parse_args(args)
    granularity = Granularity(params.granularity)
    if params.end is None:
        params.end = params.start + granularity.to_range()

    start, end = granularity.round(params.start.date()), granularity.round(params.end.date())
    if start > end:
        msg = "Start date must be less than end date."
        raise ValueError(msg)

    logger.info("Creating partitions from %s to %s with %s granularity", start, end, granularity.value)

    session_factory = create_session_factory(settings.database)
    async with session_factory() as session:
        for from_, to in generate_partition_range(start, end, granularity):
            await create_partition(from_, to, granularity, session)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
