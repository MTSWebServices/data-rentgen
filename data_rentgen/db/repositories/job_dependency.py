# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import ARRAY, Integer, bindparam, cast, func, select, tuple_

from data_rentgen.db.models.job_dependency import JobDependency
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDependencyDTO

fetch_bulk_query = select(JobDependency).where(
    tuple_(JobDependency.from_job_id, JobDependency.to_job_id).in_(
        select(
            func.unnest(
                cast(bindparam("from_job_ids"), ARRAY(Integer())),
                cast(bindparam("to_job_ids"), ARRAY(Integer())),
            )
            .table_valued("from_job_ids", "to_job_ids")
            .render_derived(),
        ),
    ),
)

get_one_query = select(JobDependency).where(
    JobDependency.from_job_id == bindparam("from_job_id"),
    JobDependency.to_job_id == bindparam("to_job_id"),
)


class JobDependencyRepository(Repository[JobDependency]):
    async def fetch_bulk(
        self,
        job_dependencies_dto: list[JobDependencyDTO],
    ) -> list[tuple[JobDependencyDTO, JobDependency | None]]:
        if not job_dependencies_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "from_job_ids": [item.from_job.id for item in job_dependencies_dto],
                "to_job_ids": [item.to_job.id for item in job_dependencies_dto],
            },
        )
        existing = {(item.from_job_id, item.to_job_id): item for item in scalars.all()}
        return [
            (
                dto,
                existing.get((dto.from_job.id, dto.to_job.id)),  # type: ignore[arg-type]
            )
            for dto in job_dependencies_dto
        ]

    async def create(self, job_dependency: JobDependencyDTO) -> JobDependency:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(job_dependency.from_job.id, job_dependency.to_job.id)
        return await self._get(job_dependency) or await self._create(job_dependency)

    async def _get(self, job_dependency: JobDependencyDTO) -> JobDependency | None:
        return await self._session.scalar(
            get_one_query,
            {
                "from_job_id": job_dependency.from_job.id,
                "to_job_id": job_dependency.to_job.id,
            },
        )

    async def _create(self, job_dependency: JobDependencyDTO) -> JobDependency:
        result = JobDependency(
            from_job_id=job_dependency.from_job.id,
            to_job_id=job_dependency.to_job.id,
            type=job_dependency.type,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result
