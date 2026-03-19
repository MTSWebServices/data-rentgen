# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from data_rentgen.db.models import User
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.schemas.v1 import (
    LineageResponseV1,
    OperationDetailedResponseV1,
    OperationLineageQueryV1,
    OperationQueryV1,
    PageResponseV1,
)
from data_rentgen.server.services import LineageService, OperationService, get_user
from data_rentgen.server.utils.lineage_response import build_lineage_response
from data_rentgen.services import UnitOfWork

router = APIRouter(
    prefix="/operations",
    tags=["Operations"],
    responses=get_error_responses(include={InvalidRequestSchema}),
)


@router.get("", summary="Paginated list of Operations")
async def operations(
    query_args: Annotated[OperationQueryV1, Query()],
    unit_of_work: Annotated[UnitOfWork, Depends()],
    operation_service: Annotated[OperationService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> PageResponseV1[OperationDetailedResponseV1]:
    pagination = await operation_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        since=query_args.since,
        until=query_args.until,
        operation_ids=query_args.operation_id,
        run_ids=query_args.run_id,
    )
    return PageResponseV1[OperationDetailedResponseV1].from_pagination(pagination)


@router.get("/lineage", summary="Get Operation lineage graph", tags=["Lineage"])
async def get_operations_lineage(
    query_args: Annotated[OperationLineageQueryV1, Query()],
    lineage_service: Annotated[LineageService, Depends()],
    current_user: Annotated[User, Depends(get_user())],
) -> LineageResponseV1:
    lineage = await lineage_service.get_lineage_by_operations(
        start_node_ids=[query_args.start_node_id],
        direction=query_args.direction,
        since=query_args.since,
        until=query_args.until,
        depth=query_args.depth,
        include_column_lineage=query_args.include_column_lineage,
    )

    return build_lineage_response(lineage)
