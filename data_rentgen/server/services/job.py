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

    def merge(self, other: "JobHierarchyResult") -> "JobHierarchyResult":
        self.parents.update(other.parents)
        self.dependencies.update(other.dependencies)
        self.jobs.extend(other.jobs)
        return self


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
        start_node_ids: set[int],
        direction: Literal["UPSTREAM", "DOWNSTREAM", "BOTH"],
        depth: int,
        since: datetime | None = None,
        until: datetime | None = None,
        *,
        infer_from_lineage: bool = False,
        level: int = 0,
    ) -> JobHierarchyResult:
        if not start_node_ids:
            return JobHierarchyResult()

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get hierarchy by jobs %r, with direction %s, since %s, until %s",
                level,
                sorted(start_node_ids),
                direction,
                since,
                until,
            )

        # Add ancestors and descendants for current level
        ancestor_relations = await self._uow.job.list_ancestor_relations(start_node_ids)
        descendant_relations = await self._uow.job.list_descendant_relations(start_node_ids)
        job_ids = (
            start_node_ids
            | {r.parent_job_id for r in ancestor_relations}
            | {r.child_job_id for r in descendant_relations}
        )
        result = JobHierarchyResult()
        result.parents.update(ancestor_relations)
        result.parents.update(descendant_relations)

        upstream_dependecies = set()
        downstream_dependencies = set()
        if direction in {"UPSTREAM", "BOTH"}:
            upstream_dependecies.update(
                await self._uow.job_dependency.get_dependencies(
                    job_ids=list(job_ids),
                    direction="UPSTREAM",
                    infer_from_lineage=infer_from_lineage,
                    since=since,
                    until=until,
                )
            )

        if direction in {"DOWNSTREAM", "BOTH"}:
            downstream_dependencies.update(
                await self._uow.job_dependency.get_dependencies(
                    job_ids=list(job_ids),
                    direction="DOWNSTREAM",
                    infer_from_lineage=infer_from_lineage,
                    since=since,
                    until=until,
                )
            )

        result.dependencies.update(
            {(d.from_job_id, d.to_job_id, d.type) for d in upstream_dependecies}
            | {(d.from_job_id, d.to_job_id, d.type) for d in downstream_dependencies}
        )

        if depth > 1:
            result.merge(
                await self.get_jobs_hierarchy(
                    start_node_ids={d.from_job_id for d in upstream_dependecies},
                    direction="UPSTREAM",
                    depth=depth - 1,
                    infer_from_lineage=infer_from_lineage,
                    since=since,
                    until=until,
                    level=level + 1,
                )
            )
            result.merge(
                await self.get_jobs_hierarchy(
                    start_node_ids={d.to_job_id for d in downstream_dependencies},
                    direction="DOWNSTREAM",
                    depth=depth - 1,
                    infer_from_lineage=infer_from_lineage,
                    since=since,
                    until=until,
                    level=level + 1,
                )
            )

        # After all recursive calls are merged
        if level == 0:
            # Add parents for all dependencies
            dependency_job_ids = {from_job_id for (from_job_id, _, _) in result.dependencies} | {
                to_job_id for (_, to_job_id, _) in result.dependencies
            }
            existing_parent_job_ids = {from_job_id for (from_job_id, _) in result.parents} | {
                to_job_id for (_, to_job_id) in result.parents
            }
            fetch_parent_job_ids = dependency_job_ids - existing_parent_job_ids
            result.parents.update(await self._uow.job.list_ancestor_relations(fetch_parent_job_ids))
            result.parents.update(await self._uow.job.list_descendant_relations(fetch_parent_job_ids))

            # Collect all job nodes once
            job_ids = (
                start_node_ids
                | dependency_job_ids
                | {from_job_id for (from_job_id, _) in result.parents}
                | {to_job_id for (_, to_job_id) in result.parents}
            )
            result.jobs = [JobServiceResult.from_orm(job) for job in await self._uow.job.list_by_ids(job_ids)]

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d parents, %d dependencies",
                level,
                len(result.jobs),
                len(result.parents),
                len(result.dependencies),
            )
        return result
