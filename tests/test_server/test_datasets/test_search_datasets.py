from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import dataset_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_datasets

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_datasets_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    _, _, datasets_by_address = datasets_search
    # dataset with id 8 has two addresses urls: [hdfs://my-cluster-namenode:2080, hdfs://my-cluster-namenode:8020] and random name
    datasets = await enrich_datasets([datasets_by_address["hdfs://my-cluster-namenode:2080"]], async_session)

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        # search by word prefix
        params={"search_query": "nameno"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    datasets_by_name, datasets_by_location, _ = datasets_search
    # Datasets with ids 3 and 4 has location names `postgres.location` and `postgres.history_location`
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    # So this test also cover case: `search in dataset.name + location.name`
    datasets = await enrich_datasets(
        [
            # on top of the search are results with shorter name,
            # then sorted alphabetically
            datasets_by_location["postgres.history_location"],
            datasets_by_location["postgres.location"],
            datasets_by_name["postgres.public.location_history"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "postgres.location"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 3,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_location_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    _, datasets_by_location, _ = datasets_search
    # Dataset with id 6 has location with external id `abc123`
    datasets = await enrich_datasets(
        [
            datasets_by_location["with-external-id"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "abc123"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_dataset_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    datasets_by_name, _, _ = datasets_search
    # Dataset with id 1 has dataset name `postgres.public.location_history`
    datasets = await enrich_datasets([datasets_by_name["postgres.public.location_history"]], async_session)

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "location_history"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    _, datasets_by_location, datasets_by_address = datasets_search
    # Dataset with id 5 has location name `my-cluster`
    # Dataset with id 8 has address url `hdfs://my-cluster-namenode:2080` and `hdfs://my-cluster-namenode:8020`
    datasets = await enrich_datasets(
        [
            datasets_by_location["my-cluster"],
            datasets_by_address["hdfs://my-cluster-namenode:8020"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "my-cluster"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 2,
        },
        "items": [
            {
                "id": str(dataset.id),
                "data": dataset_to_json(dataset),
                "tags": tag_values_to_json(dataset.tag_values) if dataset.tag_values else [],
            }
            for dataset in datasets
        ],
    }


async def test_search_datasets_no_results(
    test_client: AsyncClient,
    datasets_search: tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "not-found"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 0,
        },
        "items": [],
    }
