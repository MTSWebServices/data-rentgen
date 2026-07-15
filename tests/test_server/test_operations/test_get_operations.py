from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Run
from data_rentgen.utils.uuid import generate_new_uuid
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]

EMPTY_STATS = {
    "inputs": {
        "total_datasets": 0,
        "total_bytes": 0,
        "total_rows": 0,
        "total_files": 0,
    },
    "outputs": {
        "total_datasets": 0,
        "total_bytes": 0,
        "total_rows": 0,
        "total_files": 0,
    },
}


async def test_get_operations_missing_fields(test_client: AsyncClient, mocked_user: MockedUser):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query"],
                    "code": "value_error",
                    "message": "Value error, input should contain either 'run_id' or 'operation_id' field",
                    "context": {},
                    "input": {
                        "operation_id": [],
                        "page_size": 20,
                        "page": 1,
                        "run_id": [],
                    },
                },
            ],
        },
    }


async def test_get_operations_until_less_than_since(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "run_id": str(new_run.id),
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


async def test_get_operations_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/operations", params={"operation_id": str(generate_new_uuid())})

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }
