# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import Row

from data_rentgen.db.models import Job, Run, RunStatus
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class RunServiceIOStatistics:
    total_datasets: int = 0
    total_bytes: int = 0
    total_rows: int = 0
    total_files: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(
            total_datasets=row.total_datasets,
            total_bytes=row.total_bytes or 0,
            total_rows=row.total_rows or 0,
            total_files=row.total_files or 0,
        )


@dataclass
class RunServiceOperationStatistics:
    total_operations: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(
            total_operations=row.total_operations,
        )


@dataclass
class RunServiceStatistics:
    inputs: RunServiceIOStatistics
    outputs: RunServiceIOStatistics
    operations: RunServiceOperationStatistics


@dataclass
class RunServicePageItem:
    id: UUID
    data: Run
    job: Job
    statistics: RunServiceStatistics


class RunServicePaginatedResult(PaginationDTO[RunServicePageItem]):
    pass


class RunService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        since: datetime | None,
        until: datetime | None,
        run_ids: Collection[UUID],
        job_ids: Collection[int],
        parent_run_ids: Collection[UUID],
        job_types: Collection[str],
        job_location_ids: Collection[int],
        statuses: Collection[str],
        started_by_users: Collection[str],
        started_since: datetime | None,
        started_until: datetime | None,
        ended_since: datetime | None,
        ended_until: datetime | None,
        search_query: str | None,
    ) -> RunServicePaginatedResult:
        pagination = await self._uow.run.paginate(
            page=page,
            page_size=page_size,
            since=since,
            until=until,
            run_ids=run_ids,
            parent_run_ids=parent_run_ids,
            job_ids=job_ids,
            job_types=job_types,
            job_location_ids=job_location_ids,
            statuses=[RunStatus[s] for s in statuses],
            started_by_users=started_by_users,
            started_since=started_since,
            started_until=started_until,
            ended_since=ended_since,
            ended_until=ended_until,
            search_query=search_query,
        )
        run_ids = [item.id for item in pagination.items]
        input_stats = await self._uow.input.get_stats_by_run_ids(run_ids)
        output_stats = await self._uow.output.get_stats_by_run_ids(run_ids)
        operation_stats = await self._uow.operation.get_stats_by_run_ids(run_ids)

        return RunServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                RunServicePageItem(
                    id=run.id,
                    data=run,
                    job=run.job,
                    statistics=RunServiceStatistics(
                        inputs=RunServiceIOStatistics.from_row(input_stats.get(run.id)),
                        outputs=RunServiceIOStatistics.from_row(output_stats.get(run.id)),
                        operations=RunServiceOperationStatistics.from_row(operation_stats.get(run.id)),
                    ),
                )
                for run in pagination.items
            ],
        )
