from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import dataset_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_datasets_no_filters(
    test_client: AsyncClient,
    datasets: list[Dataset],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    datasets = await enrich_datasets(datasets, async_session)
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(datasets),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in sorted(datasets, key=lambda x: x.name.lower())
        ],
    }


async def test_get_datasets_unauthorized(test_client: AsyncClient):
    response = await test_client.get("v1/datasets")
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_datasets_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
