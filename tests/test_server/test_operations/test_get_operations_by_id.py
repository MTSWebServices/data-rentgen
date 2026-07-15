from collections import defaultdict
from http import HTTPStatus

import pytest
from httpx import AsyncClient

from data_rentgen.db.models import Operation
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import operation_to_json
from tests.test_server.utils.lineage_result import LineageResult

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


async def test_get_operations_by_unknown_id(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"operation_id": str(new_operation.id)},
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


async def test_get_operations_by_one_id(
    test_client: AsyncClient,
    operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"operation_id": str(operation.id)},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 1,
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
            },
        ],
    }


async def test_get_operations_by_multiple_ids(
    test_client: AsyncClient,
    operations: list[Operation],
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_operations = operations[:2]

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "operation_id": [str(operation.id) for operation in selected_operations],
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
            for operation in sorted(selected_operations, key=lambda x: (x.created_at, x.id.int), reverse=True)
        ],
    }


async def test_get_operations_by_multiple_ids_with_stats(
    test_client: AsyncClient,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    input_stats = defaultdict(dict)
    output_stats = defaultdict(dict)
    for operation in simple_lineage.operations:
        input_bytes = 0
        input_rows = 0
        input_files = 0
        output_bytes = 0
        output_rows = 0
        output_files = 0

        for input in simple_lineage.inputs:
            if input.operation_id != operation.id:
                continue
            input_bytes += input.num_bytes or 0
            input_rows += input.num_rows or 0
            input_files += input.num_files or 0

        for output in simple_lineage.outputs:
            if output.operation_id != operation.id:
                continue
            output_bytes += output.num_bytes or 0
            output_rows += output.num_rows or 0
            output_files += output.num_files or 0

        input_stats[operation.id] = {
            "total_datasets": 1,
            "total_bytes": input_bytes,
            "total_rows": input_rows,
            "total_files": input_files,
        }

        output_stats[operation.id] = {
            "total_datasets": 1,
            "total_bytes": output_bytes,
            "total_rows": output_rows,
            "total_files": output_files,
        }

    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "operation_id": [str(operation.id) for operation in simple_lineage.operations],
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(simple_lineage.operations),
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
                "statistics": {
                    "inputs": input_stats[operation.id],
                    "outputs": output_stats[operation.id],
                },
            }
            for operation in sorted(simple_lineage.operations, key=lambda x: (x.created_at, x.id.int), reverse=True)
        ],
    }


async def test_get_operations_by_one_id_with_sql_query(
    test_client: AsyncClient,
    operation_with_sql_query: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"operation_id": str(operation_with_sql_query.id)},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 1,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(operation_with_sql_query.id),
                "data": operation_to_json(operation_with_sql_query),
                "statistics": EMPTY_STATS,
            },
        ],
    }


async def test_get_operations_via_personal_token_is_allowed(
    test_client: AsyncClient,
    new_operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={"operation_id": str(new_operation.id)},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
