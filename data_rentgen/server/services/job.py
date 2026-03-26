# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from itertools import groupby
from typing import Annotated, Literal

from fastapi import Depends

from data_rentgen.db.models import Job, Location, Run
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.server.services.tag import TagData, TagValueData
from data_rentgen.services.uow import UnitOfWork

logger = logging.getLogger(__name__)


@dataclass
class JobData:
    id: int
    parent_job_id: int | None
    name: str
    type: str
    location: Location


@dataclass
class JobServiceResult:
    id: int
    data: JobData
    tags: list[TagData]
    last_run: Run | None

    @classmethod
    def from_orm(cls, job: Job):
        return JobServiceResult(
            id=job.id,
            data=JobData(
                id=job.id,
                parent_job_id=job.parent_job_id,
                name=job.name,
                type=job.type,
                location=job.location,
            ),
            last_run=job.last_run,  # type: ignore[attr-defined]
            tags=[
                TagData(
                    id=tag.id,
                    name=tag.name,
                    values=[TagValueData(id=tv.id, value=tv.value) for tv in sorted(group, key=lambda tv: tv.value)],
                )
                for tag, group in groupby(
                    sorted(job.tag_values, key=lambda tv: tv.tag.name),
                    key=lambda tv: tv.tag,
                )
            ],
        )


class JobServicePaginatedResult(PaginationDTO[JobServiceResult]):
    pass


@dataclass
class JobHierarchyResult:
    parents: set[tuple[int, int]] = field(default_factory=set)
    dependencies: set[tuple[int, int, str | None]] = field(default_factory=set)
    jobs: list[JobServiceResult] = field(default_factory=list)


class JobService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: Collection[int],
        parent_job_ids: Collection[int],
        job_types: Collection[str],
        tag_value_ids: Collection[int],
        location_ids: Collection[int],
        location_types: Collection[str],
        search_query: str | None,
    ) -> JobServicePaginatedResult:
        pagination = await self._uow.job.paginate(
            page=page,
            page_size=page_size,
            job_ids=job_ids,
            parent_job_ids=parent_job_ids,
            job_types=job_types,
            tag_value_ids=tag_value_ids,
            location_ids=location_ids,
            location_types=location_types,
            search_query=search_query,
        )

        return JobServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[JobServiceResult.from_orm(job) for job in pagination.items],
        )

    async def get_job_types(self) -> Sequence[str]:
        return await self._uow.job_type.get_job_types()

    async def get_jobs_hierarchy(
        self,
        start_node_id: int,
        direction: Literal["UPSTREAM", "DOWNSTREAM", "BOTH"],
        depth: int,
        since: datetime | None = None,
        until: datetime | None = None,
        *,
        infer_from_lineage: bool = False,
    ) -> JobHierarchyResult:
        logger.info(
            "Get jobs hierarchy with start at job with id %s, direction %s, depth %s",
            start_node_id,
            direction,
            depth,
        )
        job_ids = {start_node_id}

        ancestor_relations = await self._uow.job.list_ancestor_relations([start_node_id])
        descendant_relations = await self._uow.job.list_descendant_relations([start_node_id])
        job_ids = (
            {start_node_id}
            | {p.parent_job_id for p in ancestor_relations}
            | {p.child_job_id for p in descendant_relations}
        )

        dependencies = await self._uow.job_dependency.get_dependencies(
            job_ids=list(job_ids),
            direction=direction,
            depth=depth,
            infer_from_lineage=infer_from_lineage,
            since=since,
            until=until,
        )
        dependency_job_ids = {d.from_job_id for d in dependencies} | {d.to_job_id for d in dependencies}
        job_ids |= dependency_job_ids
        # return ancestors for all found jobs in the graph
        ancestor_relations += await self._uow.job.list_ancestor_relations(list(dependency_job_ids))
        job_ids |= {p.parent_job_id for p in ancestor_relations}
        jobs = await self._uow.job.list_by_ids(list(job_ids))
        return JobHierarchyResult(
            parents={(p.parent_job_id, p.child_job_id) for p in ancestor_relations + descendant_relations},
            dependencies={
                (d.from_job_id, d.to_job_id, d.type)
                for d in sorted(dependencies, key=lambda x: (x.from_job_id, x.to_job_id, x.type))
            },
            jobs=[JobServiceResult.from_orm(job) for job in jobs],
        )
