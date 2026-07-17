from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Operation, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import operation_to_json

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


async def test_get_operations_by_unknown_run_id(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": new_operation.created_at.isoformat(),
            "run_id": str(new_operation.run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 0,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [],
    }


async def test_get_operations_by_run_id(
    test_client: AsyncClient,
    runs: list[Run],
    operations: list[Operation],
    mocked_user: MockedUser,
):
    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in runs if run.id in run_ids]
    selected_run = runs[0]

    selected_operations = [operation for operation in operations if operation.run_id == selected_run.id]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "run_id": str(selected_run.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(selected_operations),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(operation.id),
                "data": operation_to_json(operation),
                "statistics": EMPTY_STATS,
            }
            for operation in sorted(selected_operations, key=lambda x: (x.id, x.created_at), reverse=True)
        ],
    }


async def test_get_operations_by_run_id_with_since(
    test_client: AsyncClient,
    operations_with_same_run: list[Operation],
    mocked_user: MockedUser,
):
    since = min(operation.created_at for operation in operations_with_same_run) + timedelta(seconds=1)
    selected_operations = [operation for operation in operations_with_same_run if since <= operation.created_at]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "run_id": str(operations_with_same_run[0].run_id),
            "since": since.isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(selected_operations),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(operation.id),
                "data": operation_to_json(operation),
                "statistics": EMPTY_STATS,
            }
            for operation in sorted(selected_operations, key=lambda x: (x.id, x.created_at), reverse=True)
        ],
    }


async def test_get_operations_by_run_id_with_until(
    test_client: AsyncClient,
    operations_with_same_run: list[Operation],
    mocked_user: MockedUser,
):
    until = min(operation.created_at for operation in operations_with_same_run) + timedelta(seconds=1)
    selected_operations = [operation for operation in operations_with_same_run if operation.created_at <= until]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "run_id": str(operations_with_same_run[0].run_id),
            "until": until.isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 2,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(operation.id),
                "data": operation_to_json(operation),
                "statistics": EMPTY_STATS,
            }
            for operation in sorted(selected_operations, key=lambda x: (x.id, x.created_at), reverse=True)
        ],
    }
