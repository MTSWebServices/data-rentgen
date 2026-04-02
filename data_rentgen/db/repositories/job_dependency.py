# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Literal

from sqlalchemy import (
    ARRAY,
    CTE,
    CompoundSelect,
    DateTime,
    Integer,
    Select,
    and_,
    any_,
    bindparam,
    cast,
    func,
    literal,
    or_,
    select,
    tuple_,
)

from data_rentgen.db.models.input import Input
from data_rentgen.db.models.job_dependency import JobDependency
from data_rentgen.db.models.output import Output
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

    async def get_dependencies(
        self,
        job_ids: list[int],
        direction: Literal["UPSTREAM", "DOWNSTREAM"],
        depth: int,
        since: datetime | None = None,
        until: datetime | None = None,
        *,
        infer_from_lineage: bool = False,
    ) -> list[JobDependency]:
        core_query = self._get_core_hierarchy_query(include_indirect=infer_from_lineage)

        query: Select | CompoundSelect
        match direction:
            case "UPSTREAM":
                query = self._get_upstream_hierarchy_query(core_query)
            case "DOWNSTREAM":
                query = self._get_downstream_hierarchy_query(core_query)

        result = await self._session.execute(
            query, {"job_ids": job_ids, "depth": depth, "since": since, "until": until}
        )
        return [
            JobDependency(from_job_id=item.from_job_id, to_job_id=item.to_job_id, type=item.type)
            for item in result.all()
        ]

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

    def _get_core_hierarchy_query(
        self,
        *,
        include_indirect: bool = False,
    ) -> CTE:
        query: Select | CompoundSelect
        query = select(
            JobDependency.from_job_id,
            JobDependency.to_job_id,
            JobDependency.type,
        )
        if include_indirect:
            query = query.union(
                select(
                    Output.job_id.label("from_job_id"),
                    Input.job_id.label("to_job_id"),
                    literal("INFERRED_FROM_LINEAGE").label("type"),
                )
                .distinct()
                .join(
                    Input,
                    and_(
                        Output.dataset_id == Input.dataset_id,
                        Output.created_at >= Input.created_at,
                        Output.job_id != Input.job_id,
                    ),
                )
                .where(
                    Input.created_at >= bindparam("since"),
                    or_(
                        bindparam("until", type_=DateTime(timezone=True)).is_(None),
                        Output.created_at <= bindparam("until"),
                    ),
                )
            )
        return query.cte("jobs_hierarchy_core_query")

    def _get_upstream_hierarchy_query(
        self,
        core_query: CTE,
    ) -> Select:
        base_part = select(
            core_query.c.from_job_id.label("from_job_id"),
            core_query.c.to_job_id.label("to_job_id"),
            core_query.c.type.label("type"),
            literal(1).label("depth"),
        ).where(core_query.c.to_job_id == any_(bindparam("job_ids")))

        base_query_cte = base_part.cte(name="upstream_jobs_query", recursive=True)

        recursive_part = (
            select(
                core_query.c.from_job_id.label("from_job_id"),
                core_query.c.to_job_id.label("to_job_id"),
                core_query.c.type.label("type"),
                (base_query_cte.c.depth + 1).label("depth"),
            )
            .join(
                core_query,
                core_query.c.to_job_id == base_query_cte.c.from_job_id,
            )
            .where(
                base_query_cte.c.depth < bindparam("depth"),
            )
        )

        return select(base_query_cte.union(recursive_part))

    def _get_downstream_hierarchy_query(
        self,
        core_query: CTE,
    ) -> Select:
        base_part = select(
            core_query.c.from_job_id.label("from_job_id"),
            core_query.c.to_job_id.label("to_job_id"),
            core_query.c.type.label("type"),
            literal(1).label("depth"),
        ).where(core_query.c.from_job_id == any_(bindparam("job_ids")))

        base_part_cte = base_part.cte(name="downstream_jobs_query", recursive=True)

        recursive_part = (
            select(
                core_query.c.from_job_id.label("from_job_id"),
                core_query.c.to_job_id.label("to_job_id"),
                core_query.c.type.label("type"),
                (base_part_cte.c.depth + 1).label("depth"),
            )
            .join(
                core_query,
                core_query.c.from_job_id == base_part_cte.c.to_job_id,
            )
            .where(
                base_part_cte.c.depth < bindparam("depth"),
            )
        )

        return select(base_part_cte.union(recursive_part))
