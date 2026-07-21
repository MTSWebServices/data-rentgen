# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends

from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.services.uow import UnitOfWork


@dataclass(slots=True)
class TagValueData:
    id: int
    value: str


@dataclass(slots=True)
class TagData:
    id: int
    name: str
    values: list[TagValueData]


@dataclass(slots=True)
class TagServiceResult:
    id: int
    data: TagData


class TagServicePaginatedResult(PaginationDTO[TagServiceResult]):
    pass


class TagService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        tag_ids: list[int],
        search_query: str | None,
    ) -> TagServicePaginatedResult:
        pagination = await self._uow.tag.paginate(
            page=page,
            page_size=page_size,
            tag_ids=tag_ids,
            search_query=search_query,
        )

        return TagServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[
                TagServiceResult(
                    id=tag.id,
                    data=TagData(
                        id=tag.id,
                        name=tag.name,
                        values=[TagValueData(id=tv.id, value=tv.value) for tv in tag.tag_values],
                    ),
                )
                for tag in pagination.items
            ],
        )
