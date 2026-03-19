# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends
from sqlalchemy import Row

from data_rentgen.db.models import Location
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass
class LocationServiceDatasetStatistics:
    total_datasets: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(total_datasets=row.total_datasets)


@dataclass
class LocationServiceJobStatistics:
    total_jobs: int = 0

    @classmethod
    def from_row(cls, row: Row | None):
        if not row:
            return cls()

        return cls(total_jobs=row.total_jobs)


@dataclass
class LocationServiceStatistics:
    datasets: LocationServiceDatasetStatistics
    jobs: LocationServiceJobStatistics


@dataclass
class LocationServiceResult:
    id: int
    data: Location
    statistics: LocationServiceStatistics


class LocationServicePaginatedResult(PaginationDTO[LocationServiceResult]):
    pass


class LocationService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def __aenter__(self):
        await self._uow.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._uow.__aexit__(exc_type, exc_value, traceback)

    async def paginate(
        self,
        page: int,
        page_size: int,
        location_ids: Collection[int],
        location_type: Collection[str],
        search_query: str | None,
    ) -> LocationServicePaginatedResult:
        pagination = await self._uow.location.paginate(
            page=page,
            page_size=page_size,
            location_ids=location_ids,
            location_type=location_type,
            search_query=search_query,
        )
        location_ids = [item.id for item in pagination.items]
        dataset_stats = await self._uow.dataset.get_stats_by_location_ids(location_ids)
        job_stats = await self._uow.job.get_stats_by_location_ids(location_ids)

        return LocationServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                LocationServiceResult(
                    id=location.id,
                    data=location,
                    statistics=LocationServiceStatistics(
                        datasets=LocationServiceDatasetStatistics.from_row(dataset_stats.get(location.id)),
                        jobs=LocationServiceJobStatistics.from_row(job_stats.get(location.id)),
                    ),
                )
                for location in pagination.items
            ],
        )

    async def update_external_id(
        self,
        location_id: int,
        external_id: str | None,
    ) -> LocationServiceResult:
        location = await self._uow.location.update_external_id(location_id, external_id)
        dataset_stats = await self._uow.dataset.get_stats_by_location_ids([location.id])
        job_stats = await self._uow.job.get_stats_by_location_ids([location.id])
        return LocationServiceResult(
            id=location.id,
            data=location,
            statistics=LocationServiceStatistics(
                datasets=LocationServiceDatasetStatistics.from_row(dataset_stats.get(location.id)),
                jobs=LocationServiceJobStatistics.from_row(job_stats.get(location.id)),
            ),
        )

    async def get_location_types(self) -> Sequence[str]:
        return await self._uow.location.get_location_types()
