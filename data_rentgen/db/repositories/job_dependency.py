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

from data_rentgen.db.models.dataset_symlink import DatasetSymlink
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

    async def get_dependencies(
        self,
        job_ids: list[int],
        direction: Literal["UPSTREAM", "DOWNSTREAM"],
        since: datetime | None = None,
        until: datetime | None = None,
        *,
        infer_from_lineage: bool = False,
    ) -> list[JobDependency]:
        core_query = self._get_core_hierarchy_query(include_indirect=infer_from_lineage)

        query: Select | CompoundSelect
        match direction:
            case "UPSTREAM":
                query = select(core_query).where(core_query.c.to_job_id == any_(bindparam("job_ids")))
            case "DOWNSTREAM":
                query = select(core_query).where(core_query.c.from_job_id == any_(bindparam("job_ids")))

        result = await self._session.execute(
            query,
            {"job_ids": job_ids, "since": since, "until": until},
        )
        return [
            JobDependency(from_job_id=item.from_job_id, to_job_id=item.to_job_id, type=item.type)
            for item in result.all()
        ]

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
            datasets_connected_via_symlink = (
                select(literal(1))
                .where(
                    or_(
                        and_(
                            DatasetSymlink.from_dataset_id == Output.dataset_id,
                            DatasetSymlink.to_dataset_id == Input.dataset_id,
                        ),
                        and_(
                            DatasetSymlink.to_dataset_id == Output.dataset_id,
                            DatasetSymlink.from_dataset_id == Input.dataset_id,
                        ),
                    ),
                )
                .exists()
            )
            where_clauses = [
                Input.created_at >= bindparam("since"),
                Output.created_at >= bindparam("since"),
                Output.created_at >= Input.created_at,
                Output.job_id != Input.job_id,
                or_(
                    bindparam("until", type_=DateTime(timezone=True)).is_(None),
                    and_(
                        Input.created_at <= bindparam("until"),
                        Output.created_at <= bindparam("until"),
                    ),
                ),
            ]
            direct_connection = (
                select(
                    Output.job_id.label("from_job_id"),
                    Input.job_id.label("to_job_id"),
                    literal("INFERRED_FROM_LINEAGE").label("type"),
                )
                .distinct()
                .join(
                    Input,
                    Output.dataset_id == Input.dataset_id,
                )
                .where(*where_clauses)
            )
            via_symlinks = (
                select(
                    Output.job_id.label("from_job_id"),
                    Input.job_id.label("to_job_id"),
                    literal("INFERRED_FROM_LINEAGE").label("type"),
                )
                .distinct()
                .join(
                    Input,
                    datasets_connected_via_symlink,
                )
                .where(*where_clauses)
            )

            query = query.union(direct_connection, via_symlinks)

        return query.cte("jobs_hierarchy_core_query").prefix_with("NOT MATERIALIZED", dialect="postgresql")
