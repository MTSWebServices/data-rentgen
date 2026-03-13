# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    JobDependenciesQueryV1,
    JobDependenciesRelationsV1,
    JobDependenciesResponseV1,
    JobDetailedResponseV1,
    JobLineageQueryV1,
    JobPaginateQueryV1,
    JobResponseV1,
    JobTypesResponseV1,
    LineageResponseV1,
    PageResponseV1,
)
from data_rentgen.server.schemas.v1.job import JobDependencyV1, JobEntityRelationV1, JobEntityV1
from data_rentgen.server.services import JobService, LineageService, get_user
from data_rentgen.server.utils.lineage_response import build_lineage_response

router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    responses=get_error_responses(
        include={
            NotAuthorizedSchema,
            NotAuthorizedRedirectSchema,
            InvalidRequestSchema,
        },
    ),
)


@router.get("", summary="Paginated list of Jobs")
async def paginate_jobs(
    query_args: Annotated[JobPaginateQueryV1, Query()],
    job_service: Annotated[JobService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[JobDetailedResponseV1]:
    pagination = await job_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        job_ids=query_args.job_id,
        parent_job_ids=query_args.parent_job_id,
        job_types=query_args.job_type,
        tag_value_ids=query_args.tag_value_id,
        location_ids=query_args.location_id,
        location_types=query_args.location_type,
        search_query=query_args.search_query,
    )
    return PageResponseV1[JobDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Job lineage graph")
async def get_jobs_lineage(
    query_args: Annotated[JobLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_jobs(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
        include_column_lineage=query_args.include_column_lineage,
    )

    return build_lineage_response(lineage)


@router.get("/types", summary="Get distinct types of Jobs")
async def get_job_types(
    job_service: Annotated[JobService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> JobTypesResponseV1:
    job_types = await job_service.get_job_types()
    return JobTypesResponseV1(job_types=list(job_types))


@router.get("/dependencies", summary="Get ancestors and descendans of the Jobs")
async def get_job_dependencies(
    query_args: Annotated[JobDependenciesQueryV1, Depends()],
    job_service: Annotated[JobService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> JobDependenciesResponseV1:
    job_dependencies = await job_service.get_job_dependencies(query_args.start_node_id, query_args.direction)
    return JobDependenciesResponseV1(
        relations=JobDependenciesRelationsV1(
            parents=[
                JobEntityRelationV1(
                    from_=JobEntityV1(id=str(from_id)),
                    to=JobEntityV1(id=str(to_id)),
                )
                for from_id, to_id in sorted(job_dependencies.parents)
            ],
            dependencies=[
                JobDependencyV1(
                    from_=JobEntityV1(id=str(from_id)),
                    to=JobEntityV1(id=str(to_id)),
                    type_=type_,
                )
                for from_id, to_id, type_ in sorted(job_dependencies.dependencies)
            ],
        ),
        nodes=[
            JobResponseV1.model_validate(job.data) for job in sorted(job_dependencies.jobs, key=lambda item: item.id)
        ],
    )
