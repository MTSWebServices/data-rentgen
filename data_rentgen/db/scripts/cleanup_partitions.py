#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import re
import sys
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Literal

from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.models import Input, Operation, Output, Run
from data_rentgen.db.models.column_lineage import ColumnLineage
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)

PARTITION_GRANULARITY_PATERN = re.compile(r"(\w+)_y(\d{4})(?:_m(\d{1,2})(?:_d(\d{1,2}))?)?")

PARTITIONED_TABLES = [
    Run.__tablename__,
    Operation.__tablename__,
    Input.__tablename__,
    Output.__tablename__,
    ColumnLineage.__tablename__,
]

POSTGRES_GET_PARTITIONS_QUERY = (
    "SELECT relname as table_name, relispartition as is_attached "
    "FROM pg_class "
    "WHERE relname like ANY(:table_prefixes) "
    "AND relkind = 'r' "
    "ORDER BY relname"
)

granularity_output_format = {
    "year": "_y%Y",
    "month": "_y%Y_m%m",
    "day": "_y%Y_m%m_d%d",
}


class Command(str, Enum):
    DRY_RUN = "dry_run"
    DETACH = "detach"
    TRUNCATE = "truncate"
    DROP = "drop"


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.cleanup_partitions truncate --keep-after 2025-01-01",
        description="Truncate or detach partitions, before provided date.",
    )
    parser.add_argument(
        "command",
        type=Command,
        choices=[item.value for item in Command],
        default=Command.DRY_RUN,
        nargs="?",
        help="Command",
    )
    parser.add_argument(
        "--keep-after",
        type=isoparse,
        default=(datetime.now(tz=UTC) - relativedelta(years=1)),
        nargs="?",
        help="Select partitions where all data is before this date",
    )
    return parser


@dataclass()
class TablePartition:
    name: str
    begin_date: date
    granularity: Literal["year", "month", "day"] = field(default="year")
    is_attached: bool = True

    @property
    def qualified_name(self):
        return self.name + self.begin_date.strftime(granularity_output_format[self.granularity])


async def get_partitioned_tables(session: AsyncSession) -> dict[str, list[TablePartition]]:
    tables: dict[str, list[TablePartition]] = defaultdict(list)
    table_prefixes = [table_name + "%" for table_name in PARTITIONED_TABLES]
    result = await session.execute(text(POSTGRES_GET_PARTITIONS_QUERY), {"table_prefixes": table_prefixes})
    table_names = result.all()
    if not table_names:
        logger.info("There is no partitioned tables")
        return {}
    # parse tabel_names
    for tabel_name, is_attached in table_names:
        granularity: Literal["year", "month", "day"] = "year"
        match = re.search(PARTITION_GRANULARITY_PATERN, tabel_name)
        if not match:
            continue

        (name, year, month, day) = match.groups()  # type: ignore[union-attr]
        year = int(year)
        if month:
            month = int(month)
            granularity = "month"
        else:
            month = 1
        if day:
            day = int(day)
            granularity = "day"
        else:
            day = 1

        tables[name].append(
            TablePartition(
                name=name,
                begin_date=date(year, month, day),
                granularity=granularity,
                is_attached=is_attached,
            ),
        )
    return tables


def filter_partitions(tables: dict[str, list[TablePartition]], keep_after: date) -> dict[str, list[TablePartition]]:
    return {
        table_name: [partition for partition in partitions if partition.begin_date < keep_after]
        for table_name, partitions in tables.items()
    }


def get_queries(tables: dict[str, list[TablePartition]], command: Command) -> list[str]:
    result: list[str] = []
    for table_name, partitions in tables.items():
        for partition in partitions:
            match command:
                case Command.DETACH if partition.is_attached:
                    query_template = f"ALTER TABLE {table_name} DETACH PARTITION {partition.qualified_name};"
                case Command.TRUNCATE if partition.is_attached:
                    query_template = f"TRUNCATE TABLE {partition.qualified_name};"
                case Command.DROP:
                    query_template = f"DROP TABLE {partition.qualified_name};"
                case _:
                    continue
            result.append(query_template)
    return result


def print_partitions(tables: dict[str, list[TablePartition]]):
    for table_name, partitions in tables.items():
        logger.info("  Partitions of table %r", table_name)
        for partition in partitions:
            logger.info("    %s", partition.qualified_name)


async def detach_partitions(tables: dict[str, list[TablePartition]], session: AsyncSession):
    queries = get_queries(tables, Command.DETACH)
    if not queries:
        logger.info("No partitions to detach!")
        return

    logger.info("Detaching partitions...")
    for query in queries:
        logger.debug("Detach partitions with query: %s", query)
        await session.execute(text(query))
        await session.commit()
    logger.info("  Done!")


async def drop_partitions(tables: dict[str, list[TablePartition]], session: AsyncSession):
    queries = get_queries(tables, Command.DROP)
    if not queries:
        logger.info("No partitions to drop!")
        return

    logger.info("Dropping partitions...")
    for query in queries:
        logger.debug("Remove table with query: %s", query)
        await session.execute(text(query))
        await session.commit()
    logger.info("  Done!")


async def truncate_partitions(tables: dict[str, list[TablePartition]], session: AsyncSession):
    queries = get_queries(tables, Command.TRUNCATE)
    if not queries:
        logger.info("No partitions to truncate!")
        return

    logger.info("Truncating partitions...")
    for query in get_queries(tables, Command.TRUNCATE):
        logger.debug("Truncate partition with query: %s", query)
        await session.execute(text(query))
        await session.commit()
    logger.info("  Done!")


async def main(args: list[str]) -> None:
    parser = get_parser()
    params = parser.parse_args(args)
    logger.debug("Starting cleanup partition script with params: %s", params)
    keep_after = params.keep_after.date()
    db_settings = DatabaseSettings()  # type: ignore[call-arg]
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        tables = await get_partitioned_tables(session)
        partitions = filter_partitions(tables, keep_after)  # type: ignore[arg-type]

        logger.info("Partitions matching --keep-after %s:", keep_after)
        print_partitions(partitions)
        match params.command:
            case Command.DRY_RUN:
                pass
            case Command.DETACH:
                await detach_partitions(partitions, session)
            case Command.DROP:
                await detach_partitions(partitions, session)
                await drop_partitions(partitions, session)
            case Command.TRUNCATE:
                await truncate_partitions(partitions, session)
            case _:
                logger.error("No such command: %s", params.command)


if __name__ == "__main__":
    setup_logging(LoggingSettings())
    asyncio.run(main(sys.argv[1:]))
