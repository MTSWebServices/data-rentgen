# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import Row

from data_rentgen.db.models import Operation
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class OperationServiceIOStatistics:
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
class OperationServiceStatistics:
    inputs: OperationServiceIOStatistics
    outputs: OperationServiceIOStatistics


@dataclass
class OperationServicePageItem:
    id: UUID
    data: Operation
    statistics: OperationServiceStatistics


class OperationServicePaginatedResult(PaginationDTO[OperationServicePageItem]):
    pass


class OperationService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        since: datetime | None,
        until: datetime | None,
        operation_ids: Collection[UUID],
        run_ids: Collection[UUID],
    ) -> OperationServicePaginatedResult:
        pagination = await self._uow.operation.paginate(
            page=page,
            page_size=page_size,
            since=since,
            until=until,
            operation_ids=operation_ids,
            run_ids=run_ids,
        )
        operation_ids = [item.id for item in pagination.items]
        input_stats = await self._uow.input.get_stats_by_operation_ids(operation_ids)
        output_stats = await self._uow.output.get_stats_by_operation_ids(operation_ids)

        return OperationServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                OperationServicePageItem(
                    id=operation.id,
                    data=operation,
                    statistics=OperationServiceStatistics(
                        inputs=OperationServiceIOStatistics.from_row(input_stats.get(operation.id)),
                        outputs=OperationServiceIOStatistics.from_row(output_stats.get(operation.id)),
                    ),
                )
                for operation in pagination.items
            ],
        )
