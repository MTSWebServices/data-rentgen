# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema, NotAuthorizedRedirectSchema, NotAuthorizedSchema
from data_rentgen.server.schemas.v1 import (
    DatasetDetailedResponseV1,
    DatasetLineageQueryV1,
    DatasetPaginateQueryV1,
    LineageResponseV1,
    PageResponseV1,
)
from data_rentgen.server.services import DatasetService, LineageService, get_user
from data_rentgen.server.utils.lineage_response import (
    build_lineage_response,
    build_lineage_response_with_dataset_granularity,
)

router = APIRouter(
    prefix="/datasets",
    tags=["Datasets"],
    responses=get_error_responses(include={NotAuthorizedSchema, NotAuthorizedRedirectSchema, InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Datasets")
async def paginate_datasets(
    query_args: Annotated[DatasetPaginateQueryV1, Query()],
    dataset_service: Annotated[DatasetService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[DatasetDetailedResponseV1]:
    pagination = await dataset_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        dataset_ids=query_args.dataset_id,
        tag_value_ids=query_args.tag_value_id,
        location_ids=query_args.location_id,
        location_types=query_args.location_type,
        search_query=query_args.search_query,
    )
    return PageResponseV1[DatasetDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Dataset lineage graph", tags=["Lineage"])
async def get_datasets_lineage(
    query_args: Annotated[DatasetLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_datasets(
        start_node_ids=[query_args.start_node_id],  # type: ignore[list-item]
        direction=query_args.direction,
        granularity=query_args.granularity,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
        include_column_lineage=query_args.include_column_lineage,
    )
    if query_args.granularity == "DATASET":
        return build_lineage_response_with_dataset_granularity(lineage)

    return build_lineage_response(lineage)
