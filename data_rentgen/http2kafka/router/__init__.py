# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated

from asgi_correlation_id import correlation_id
from fastapi import APIRouter, Body, Depends, Request, Response

from data_rentgen.db.models.user import User
from data_rentgen.http2kafka.router.gzip_route import SupportsGzipRoute
from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.openlineage.run_facets import OpenLineageParentRunFacet
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.errors.schemas import InvalidRequestSchema
from data_rentgen.server.errors.schemas.not_authorized import NotAuthorizedSchema
from data_rentgen.server.services.user import PersonalTokenPolicy, get_user

if TYPE_CHECKING:
    from faststream.kafka.publisher import DefaultPublisher

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1/openlineage",
    tags=["OpenLineage"],
    responses=get_error_responses(include={InvalidRequestSchema, NotAuthorizedSchema}),
    route_class=SupportsGzipRoute,
)


@router.post(
    "",
    status_code=HTTPStatus.CREATED,
    summary="API endpoint compatible with OpenLineage HttpTransport",
    description="Accepts [OpenLineage run event](https://openlineage.io/docs/spec/object-model) and sends it to Kafka",
)
async def send_events_to_kafka(
    event: Annotated[OpenLineageRunEvent, Body()],
    request: Request,
    current_user: Annotated[User, Depends(get_user(personal_token_policy=PersonalTokenPolicy.REQUIRE))],
):
    kafka_publisher: DefaultPublisher = request.app.state.publisher
    body_json_bytes = await request.body()
    logger.debug("Got 1 message (%dKiB)", len(body_json_bytes) / 1024)
    await kafka_publisher.publish(
        body_json_bytes,  # avoid encoding Pydantic model back to JSON, as we already have it
        key=get_run_key(event).encode("utf-8"),
        timestamp_ms=int(event.eventTime.timestamp() * 1000),
        correlation_id=correlation_id.get(),
        headers={
            "content-type": "application/json",
            "reported-by-user-name": current_user.name,
        },
        # add event to message accumulator. messages are send in a background task.
        # do not wait until the message is actually send
        no_confirm=True,
    )
    # When FastAPI receives Response object, it returns its content as is.
    # When it receives None, it uses json.dumps to build a response body, which is slower.
    return Response(status_code=HTTPStatus.CREATED)


def get_run_key(event: OpenLineageRunEvent) -> str:
    # https://github.com/OpenLineage/OpenLineage/blob/1.36.0/client/python/openlineage/client/transport/kafka.py#L91
    match event.run.facets.parent:
        case OpenLineageParentRunFacet(root=root) if root is not None:
            return f"run:{root.job.namespace}/{root.job.name}"
        case OpenLineageParentRunFacet(job=parent_job):
            return f"run:{parent_job.namespace}/{parent_job.name}"
        case _:
            return f"run:{event.job.namespace}/{event.job.name}"
