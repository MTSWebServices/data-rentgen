from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.utils.uuid import generate_new_uuid
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


@pytest.mark.parametrize(
    ["entity_kind", "granularity"],
    [
        ("operations", None),
        ("datasets", "RUN"),
        ("runs", "RUN"),
        ("jobs", "JOB"),
    ],
)
async def test_get_lineage_no_filter(
    test_client: AsyncClient,
    entity_kind: str,
    granularity: str,
    mocked_user: MockedUser,
):
    expected_response = {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "missing",
                    "context": {},
                    "input": {"depth": 1, "direction": "BOTH", "include_column_lineage": False},
                    "location": ["query", "since"],
                    "message": "Field required",
                },
                {
                    "code": "missing",
                    "context": {},
                    "input": {"depth": 1, "direction": "BOTH", "include_column_lineage": False},
                    "location": ["query", "start_node_id"],
                    "message": "Field required",
                },
            ],
            "message": "Invalid request",
        },
    }
    if granularity:
        [detail["input"].update({"granularity": granularity}) for detail in expected_response["error"]["details"]]

    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == expected_response


@pytest.mark.parametrize(
    ["entity_kind", "start_node_id"],
    [
        ("operations", generate_new_uuid()),
        ("datasets", 1),
        ("runs", generate_new_uuid()),
        ("jobs", 1),
    ],
    ids=["operations", "datasets", "runs", "jobs"],
)
async def test_get_lineage_missing_id(
    entity_kind: str,
    start_node_id: str,
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=UTC)

    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {},
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


@pytest.mark.parametrize(
    ["entity_kind", "start_node_id"],
    [("datasets", generate_new_uuid()), ("jobs", generate_new_uuid())],
    ids=["datasets", "jobs"],
)
async def test_get_lineage_start_node_id_int_type_validation(
    entity_kind: str,
    start_node_id: str,
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "int_parsing",
                    "context": {},
                    "input": str(start_node_id),
                    "location": [
                        "query",
                        "start_node_id",
                    ],
                    "message": "Input should be a valid integer, unable to parse string as an integer",
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    ["entity_kind", "start_node_id"],
    [("operations", 1), ("runs", 1)],
    ids=["operations", "runs"],
)
async def test_get_lineage_start_node_id_uuid_type_validation(
    entity_kind: str,
    start_node_id: int,
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "uuid_parsing",
                    "context": {},
                    "input": "1",
                    "location": [
                        "query",
                        "start_node_id",
                    ],
                    "message": (
                        "Input should be a valid UUID, invalid length: expected length 32 for simple format, found 1"
                    ),
                },
            ],
            "message": "Invalid request",
        },
    }


@pytest.mark.parametrize(
    ["entity_kind", "start_node_id"],
    [
        ("operations", "9f3569b7-b6e1-48df-893a-3e49482ba9e7"),
        ("runs", "9f3569b7-b6e1-48df-893a-3e49482ba9e7"),
    ],
    ids=["operations", "runs"],
)
async def test_get_lineage_start_node_id_uuid_version_validation(
    entity_kind: str,
    start_node_id: int,
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        f"v1/{entity_kind}/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": start_node_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "uuid_version",
                    "context": {
                        "expected_version": 7,
                    },
                    "input": start_node_id,
                    "location": [
                        "query",
                        "start_node_id",
                    ],
                    "message": "UUID version 7 expected",
                },
            ],
            "message": "Invalid request",
        },
    }


async def test_get_lineage_until_less_than_since(test_client: AsyncClient, mocked_user: MockedUser):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)

    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(generate_new_uuid()),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.isoformat(),
                },
            ],
        },
    }


@pytest.mark.parametrize(
    ["depth", "error_type", "error_message", "context"],
    [
        (0, "greater_than_equal", "Input should be greater than or equal to 1", {"ge": 1}),
        (14, "less_than_equal", "Input should be less than or equal to 10", {"le": 10}),
    ],
    ids=["depth=0", "depth=4"],
)
async def test_get_lineage_depth_out_of_bounds(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    depth: int,
    error_type: str,
    error_message: str,
    context: dict,
):
    since = datetime.now(tz=timezone.utc)
    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": str(generate_new_uuid()),
            "depth": depth,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "depth"],
                    "code": error_type,
                    "message": error_message,
                    "context": context,
                    "input": str(depth),
                },
            ],
        },
    }
