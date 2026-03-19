# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from itertools import groupby
from typing import Annotated

from fastapi import Depends

from data_rentgen.db.models import Location
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.server.services.tag import TagData, TagValueData
from data_rentgen.services.uow import UnitOfWork


@dataclass
class DatasetData:
    id: int
    name: str
    location: Location


@dataclass
class DatasetServiceResult:
    id: int
    data: DatasetData
    tags: list[TagData]


class DatasetServicePaginatedResult(PaginationDTO[DatasetServiceResult]):
    pass


class DatasetService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        dataset_ids: Collection[int],
        tag_value_ids: Collection[int],
        location_ids: Collection[int],
        location_types: Collection[str],
        search_query: str | None,
    ) -> DatasetServicePaginatedResult:
        pagination = await self._uow.dataset.paginate(
            page=page,
            page_size=page_size,
            dataset_ids=dataset_ids,
            tag_value_ids=tag_value_ids,
            location_ids=location_ids,
            location_types=location_types,
            search_query=search_query,
        )

        return DatasetServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                DatasetServiceResult(
                    id=dataset.id,
                    data=DatasetData(
                        id=dataset.id,
                        name=dataset.name,
                        location=dataset.location,
                    ),
                    tags=[
                        TagData(
                            id=tag.id,
                            name=tag.name,
                            values=[
                                TagValueData(id=tv.id, value=tv.value) for tv in sorted(group, key=lambda tv: tv.value)
                            ],
                        )
                        for tag, group in groupby(
                            sorted(dataset.tag_values, key=lambda tv: tv.tag.name),
                            key=lambda tv: tv.tag,
                        )
                    ],
                )
                for dataset in pagination.items
            ],
        )
