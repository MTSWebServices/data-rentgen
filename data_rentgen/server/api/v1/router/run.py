# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    LineageResponseV1,
    PageResponseV1,
    RunDetailedResponseV1,
    RunLineageQueryV1,
    RunsPaginateQueryV1,
)
from data_rentgen.server.services import LineageService, RunService, get_user
from data_rentgen.server.utils.lineage_response import build_lineage_response

router = APIRouter(
    prefix="/runs",
    tags=["Runs"],
    responses=get_error_responses(include={NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Runs")
async def runs(
    query_args: Annotated[RunsPaginateQueryV1, Query()],
    run_service: Annotated[RunService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[RunDetailedResponseV1]:
    pagination = await run_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        since=query_args.since,
        until=query_args.until,
        run_ids=query_args.run_id,
        parent_run_ids=query_args.parent_run_id,
        job_ids=query_args.job_id,
        job_types=query_args.job_type,
        job_location_ids=query_args.job_location_id,
        search_query=query_args.search_query,
        statuses=query_args.status,
        started_by_users=query_args.started_by_user,
        started_since=query_args.started_since,
        started_until=query_args.started_until,
        ended_since=query_args.ended_since,
        ended_until=query_args.ended_until,
    )
    return PageResponseV1[RunDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Run lineage graph", tags=["Lineage"])
async def get_runs_lineage(
    query_args: Annotated[RunLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_runs(
        start_node_ids=[query_args.start_node_id],
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
        include_column_lineage=query_args.include_column_lineage,
    )
    return build_lineage_response(lineage)
