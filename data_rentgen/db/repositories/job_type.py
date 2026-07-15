# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Sequence

from sqlalchemy import (
    any_,
    bindparam,
    literal,
    select,
)
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import JobType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobTypeDTO

fetch_bulk_query = (
    select(JobType)
    .where(
        JobType.type == any_(bindparam("types")),
    )
    .limit(bindparam("limit"))
)

get_one_query = (
    select(JobType)
    .where(
        JobType.type == bindparam("type"),
    )
    .limit(literal(1, literal_execute=True))
)

get_distinct_query = select(JobType.type).distinct(JobType.type).order_by(JobType.type)
insert_query = insert(JobType).on_conflict_do_nothing(index_elements=["type"])


class JobTypeRepository(Repository[JobType]):
    async def fetch_bulk(self, job_types_dto: list[JobTypeDTO]) -> list[tuple[JobTypeDTO, JobType | None]]:
        if not job_types_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "types": [job_type_dto.type for job_type_dto in job_types_dto],
                "limit": len(job_types_dto),
            },
        )
        existing = {job.type: job for job in scalars.all()}
        return [(job_type_dto, existing.get(job_type_dto.type)) for job_type_dto in job_types_dto]

    async def create(self, job_type_dto: JobTypeDTO) -> JobType:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock("job_type", job_type_dto.type)
        return await self._get(job_type_dto) or await self._create(job_type_dto)

    async def get_job_types(self) -> Sequence[str]:
        result = await self._session.scalars(get_distinct_query)
        return result.all()

    async def _get(self, job_type_dto: JobTypeDTO) -> JobType | None:
        return await self._session.scalar(get_one_query, {"type": job_type_dto.type})

    async def _create(self, job_type_dto: JobTypeDTO) -> JobType:
        await self._session.execute(
            insert_query,
            {
                "type": job_type_dto.type,
                "id": job_type_dto.id,
            },
        )
        return await self._get(job_type_dto)  # type: ignore[return-value]
