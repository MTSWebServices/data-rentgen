#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import sys
from argparse import ArgumentParser
from datetime import UTC, datetime, timedelta

from dateutil.parser import isoparse
from faker import Faker

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.consumer.saver import DatabaseSaver
from data_rentgen.db.factory import create_session_factory
from data_rentgen.db.scripts.seed.dbt import generate_dbt_run
from data_rentgen.db.scripts.seed.flink import generate_flink_run
from data_rentgen.db.scripts.seed.hive import generate_hive_run
from data_rentgen.db.scripts.seed.spark_local import generate_spark_run_local
from data_rentgen.db.scripts.seed.spark_yarn import generate_spark_run_yarn
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.dto.user import UserDTO
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(
        usage="python3 -m data_rentgen.db.scripts.seed",
        description="Seed database with some random-generated data",
    )
    parser.add_argument(
        "--start",
        type=isoparse,
        default=datetime.now(tz=UTC) - timedelta(weeks=2),
        nargs="?",
        help="Start date for new data, default is 2 weeks ago",
    )
    parser.add_argument(
        "--end",
        type=isoparse,
        default=datetime.now(tz=UTC),
        nargs="?",
        help="End date for new data, default is now",
    )
    parser.add_argument(
        "--min-runs",
        type=int,
        default=50,
        nargs="?",
        help="Minimum number of runs per job to generate, default is 50",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=100,
        nargs="?",
        help="Maximum number of runs per job to generate, default is 100",
    )
    return parser


async def main(args: list[str]) -> None:
    setup_logging(LoggingSettings())

    parser = get_parser()
    params = parser.parse_args(args)
    logger.debug("Starting seed script with params: %r", params)

    start = params.start.astimezone(UTC)
    end = params.end.astimezone(UTC)
    if start > end:
        msg = "Start date must be less than end date."
        raise ValueError(msg)

    min_runs = params.min_runs
    max_runs = params.max_runs
    if min_runs > max_runs:
        msg = "Min runs must be less than max runs."
        raise ValueError(msg)

    result = BatchExtractionResult()
    result.add_user(UserDTO(name="test"))

    logger.info("Generating example data between '%s' and '%s' ...", start, end)
    faker = Faker()
    # Flink jobs are usually very long running, so create just one per month
    data = [generate_flink_run(faker, start, end) for _ in range(1 + (end - start) // timedelta(days=30))]
    # For other job types create at least 3 runs
    data.extend(
        generate_spark_run_local(faker, start, end) for _ in range(faker.pyint(min_value=min_runs, max_value=max_runs))
    )
    data.extend(
        generate_hive_run(faker, start, end) for _ in range(faker.pyint(min_value=min_runs, max_value=max_runs))
    )
    data.extend(
        generate_spark_run_yarn(faker, start, end) for _ in range(faker.pyint(min_value=min_runs, max_value=max_runs))
    )
    data.extend(generate_dbt_run(faker, start, end) for _ in range(faker.pyint(min_value=min_runs, max_value=max_runs)))
    for item in data:
        result.merge(item)
    logger.info("  Generated: %r", result)

    db_settings = DatabaseSettings()  # type: ignore[call-arg]
    session_factory = create_session_factory(db_settings)
    async with session_factory() as session:
        saver = DatabaseSaver(session, logger)
        await saver.save(result)
    logger.info("  Done!")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
