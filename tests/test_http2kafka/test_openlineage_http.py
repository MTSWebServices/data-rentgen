import gzip
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.http2kafka, pytest.mark.asyncio]

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("resources")
AIRFLOW_JSONL_PATH = RESOURCES_PATH.joinpath("events_airflow.jsonl")


@pytest.mark.parametrize(
    ["event_content_transformation", "headers"],
    [
        pytest.param(
            lambda event: event,
            {"Content-Type": "application/json"},
            id="json",
        ),
        pytest.param(
            lambda event: gzip.compress(event.encode("utf-8")),
            {"Content-Type": "application/json", "Content-Encoding": "gzip"},
            id="gzipped json",
        ),
    ],
)
async def test_http2kafka_openlineage_run_event(
    mocked_user: MockedUser,
    http2kafka_client: AsyncClient,
    http2kafka_handler_with_messages: Any,
    event_content_transformation: Callable[[str], str | bytes],
    headers: dict[str, str],
    async_session: AsyncSession,
):
    with AIRFLOW_JSONL_PATH.open() as f:
        raw_events = f.readlines()

    for raw_event in raw_events:
        event = OpenLineageRunEvent.model_validate_json(raw_event)

        response = await http2kafka_client.post(
            "v1/openlineage",
            content=event_content_transformation(raw_event),
            headers={
                **headers,
                "Authorization": f"Bearer {mocked_user.personal_token}",
            },
        )

        assert response.status_code == HTTPStatus.CREATED, response.text

        handler, received = http2kafka_handler_with_messages
        await handler.wait_call(timeout=3)
        assert len(received) == 1

        message = received.pop()
        raw_message = message.raw_message

        assert raw_message.topic == "input.runs"
        # Using job for Airflow DAG and parent.job for Airflow Task
        assert raw_message.key == b"run:http://airflow-host:8081/mydag"
        assert raw_message.value == raw_event.encode("utf-8")  # ungzipped
        assert raw_message.timestamp == int(event.eventTime.timestamp() * 1000)
        assert raw_message.headers == [
            (
                "content-type",
                b"application/json",
            ),
            (
                "correlation_id",
                response.headers["X-Request-ID"].encode("utf-8"),
            ),
            (
                "reported-by-user-name",
                mocked_user.user.name.encode("utf-8"),
            ),
        ]


async def test_http2kafka_openlineage_malformed_event(
    mocked_user: MockedUser,
    http2kafka_client: AsyncClient,
    http2kafka_handler_with_messages: Any,
    async_session: AsyncSession,
):
    response = await http2kafka_client.post(
        "v1/openlineage",
        json={"not": "valid event"},
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.text
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["body", "eventTime"],
                    "message": "Field required",
                    "code": "missing",
                    "context": {},
                    "input": {"not": "valid event"},
                },
                {
                    "location": ["body", "eventType"],
                    "message": "Field required",
                    "code": "missing",
                    "context": {},
                    "input": {"not": "valid event"},
                },
                {
                    "location": ["body", "job"],
                    "message": "Field required",
                    "code": "missing",
                    "context": {},
                    "input": {"not": "valid event"},
                },
                {
                    "location": ["body", "run"],
                    "message": "Field required",
                    "code": "missing",
                    "context": {},
                    "input": {"not": "valid event"},
                },
            ],
        },
    }

    _, received = http2kafka_handler_with_messages
    assert not received


async def test_http2kafka_openlineage_wrong_token(
    mocked_user: MockedUser,
    http2kafka_client: AsyncClient,
    async_session: AsyncSession,
):
    with AIRFLOW_JSONL_PATH.open() as f:
        raw_event = f.readlines()[0]

    response = await http2kafka_client.post(
        "v1/openlineage",
        content=raw_event,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {mocked_user.personal_token + 'invalid'}",
        },
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.text
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "Signature verification failed",
        },
    }

    response = await http2kafka_client.post(
        "v1/openlineage",
        content=raw_event,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {mocked_user.access_token}",
        },
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.text
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "Access token was passed but PersonalToken was expected",
        },
    }


async def test_http2kafka_openlineage_missing_token(
    http2kafka_client: AsyncClient,
):
    with AIRFLOW_JSONL_PATH.open() as f:
        raw_event = f.readlines()[0]

    response = await http2kafka_client.post(
        "v1/openlineage",
        content=raw_event,
        headers={
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.text
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }
