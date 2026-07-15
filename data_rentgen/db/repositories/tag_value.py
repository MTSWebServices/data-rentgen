# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from sqlalchemy import (
    ARRAY,
    Integer,
    String,
    bindparam,
    cast,
    func,
    literal,
    select,
    tuple_,
)

from data_rentgen.db.models.tag_value import TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto.tag import TagValueDTO

fetch_bulk_query = (
    select(TagValue)
    .where(
        tuple_(TagValue.tag_id, TagValue.value_lower).in_(
            select(
                func.unnest(
                    cast(bindparam("tag_ids"), ARRAY(Integer())),
                    cast(bindparam("values"), ARRAY(String())),
                )
                .table_valued("tag_ids", "values")
                .render_derived(),
            ),
        ),
    )
    .limit(bindparam("limit"))
)

get_one_query = (
    select(TagValue)
    .where(TagValue.tag_id == bindparam("tag_id"), TagValue.value_lower == bindparam("value"))
    .limit(literal(1, literal_execute=True))
)


class TagValueRepository(Repository[TagValue]):
    async def fetch_bulk(self, tag_values_dto: list[TagValueDTO]) -> list[tuple[TagValueDTO, TagValue | None]]:
        if not tag_values_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "tag_ids": [item.tag.id for item in tag_values_dto],
                "values": [item.value for item in tag_values_dto],
                "limit": len(tag_values_dto),
            },
        )
        existing = {(v.tag_id, v.value.lower()): v for v in scalars.all()}
        return [
            (v, existing.get((v.tag.id, v.value.lower())))  # type: ignore[arg-type]
            for v in tag_values_dto
        ]

    async def create(self, tag_value_dto: TagValueDTO) -> TagValue:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(tag_value_dto.tag.id, tag_value_dto.value.lower())
        return (
            await self._get(tag_value_dto.tag.id, tag_value_dto.value)  # type: ignore[arg-type]
            or await self._create(tag_value_dto)
        )

    async def _get(self, tag_id: int, value: str) -> TagValue | None:
        return await self._session.scalar(get_one_query, {"tag_id": tag_id, "value": value.lower()})

    async def _create(self, tag_value: TagValueDTO) -> TagValue:
        result = TagValue(tag_id=tag_value.tag.id, value=tag_value.value)
        self._session.add(result)
        await self._session.flush([result])
        return result
