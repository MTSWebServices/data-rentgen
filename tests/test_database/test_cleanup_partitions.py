from __future__ import annotations

from datetime import date

import pytest

from data_rentgen.db.scripts.cleanup_partitions import TablePartition, filter_partitions, get_partitioned_tables

pytestmark = [pytest.mark.db]


class FakeResult:
    def __init__(self, rows: list[tuple[str, bool]]):
        self._rows = rows

    def all(self) -> list[tuple[str, bool]]:
        return self._rows


class FakeSession:
    def __init__(self, rows: list[tuple[str, bool]]):
        self._rows = rows

    async def execute(self, *args, **kwargs) -> FakeResult:
        return FakeResult(self._rows)


@pytest.mark.asyncio
async def test_get_partitioned_tables_keeps_partition_names():
    session = FakeSession(
        [
            ("run_y2025", True),
            ("run_y2025_m10", True),
            ("run_y2025_m10_25", True),
            ("run_y2025_m10_d26", False),
        ],
    )

    tables = await get_partitioned_tables(session)  # type: ignore[arg-type]

    assert [
        (partition.qualified_name, partition.granularity, partition.begin_date, partition.is_attached)
        for partition in tables["run"]
    ] == [
        ("run_y2025", "year", date(2025, 1, 1), True),
        ("run_y2025_m10", "month", date(2025, 10, 1), True),
        ("run_y2025_m10_25", "day", date(2025, 10, 25), True),
        ("run_y2025_m10_d26", "day", date(2025, 10, 26), False),
    ]


@pytest.mark.parametrize(
    ["granularity", "begin_date", "keep_after", "selected"],
    [
        ("month", date(2025, 10, 1), date(2025, 10, 27), False),
        ("month", date(2025, 10, 1), date(2025, 11, 1), True),
        ("year", date(2025, 1, 1), date(2026, 1, 2), True),
        ("year", date(2025, 1, 1), date(2025, 12, 31), False),
        ("day", date(2025, 10, 25), date(2025, 10, 25), False),
        ("day", date(2025, 10, 25), date(2025, 10, 26), True),
    ],
)
def test_filter_partitions_keeps_partitions_with_data_after_keep_after(
    granularity,
    begin_date: date,
    keep_after: date,
    selected: bool,
):
    partition = TablePartition(
        name="run",
        qualified_name="run_partition",
        begin_date=begin_date,
        granularity=granularity,
    )

    result = filter_partitions({"run": [partition]}, keep_after)

    assert (result["run"] == [partition]) is selected
