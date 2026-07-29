from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import location_to_json
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_locations_by_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
    mocked_user: MockedUser,
) -> None:
    _, locations_by_address = locations_search

    locations = await enrich_locations([locations_by_address["hdfs://my-cluster-namenode:8020"]], async_session)

    response = await test_client.get(
        "/v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        # search by word prefix
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
            "total_count": 1,
        },
        "items": [
            {
                "id": str(location.id),
                "data": location_to_json(location),
                "statistics": {
                    "datasets": {
                        "total_datasets": 0,
                    },
                    "jobs": {
                        "total_jobs": 0,
                    },
                },
            }
            for location in locations
        ],
    }


async def test_search_locations_by_location_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
    mocked_user: MockedUser,
) -> None:
    locations_by_name, _ = locations_search
    locations = await enrich_locations(
        [locations_by_name["postgres.public.users"]],
        async_session,
    )

    response = await test_client.get(
        "/v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "postgres.public"},
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
                "id": str(location.id),
                "data": location_to_json(location),
                "statistics": {
                    "datasets": {
                        "total_datasets": 0,
                    },
                    "jobs": {
                        "total_jobs": 0,
                    },
                },
            }
            for location in locations
        ],
    }


async def test_search_locations_by_location_name_and_address_url(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
    mocked_user: MockedUser,
) -> None:
    locations_by_location, locations_by_address = locations_search
    # Location name `my-cluster` and address url `hdfs://my-cluster-namenode:8020`
    # Location name `warehouse` and address url `hdfs://warehouse-cluster-namenode:2080`
    locations = await enrich_locations(
        [
            locations_by_location["my-cluster"],
            locations_by_address["hdfs://warehouse-cluster-namenode:2080"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "cluster"},
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
                "id": str(location.id),
                "data": location_to_json(location),
                "statistics": {
                    "datasets": {
                        "total_datasets": 0,
                    },
                    "jobs": {
                        "total_jobs": 0,
                    },
                },
            }
            for location in locations
        ],
    }


async def test_search_locations_by_name_and_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
    mocked_user: MockedUser,
) -> None:
    _, locations_by_address = locations_search
    # Location name `my-cluster` and address url `hdfs://my-cluster-namenode:8020`
    # Location name `warehouse` and address url `hdfs://warehouse-cluster-namenode:2080`
    [location] = await enrich_locations(
        [
            locations_by_address["hdfs://warehouse-cluster-namenode:2080"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "location_type": "hdfs",
            "search_query": "cluster",
        },
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
                "id": str(location.id),
                "data": location_to_json(location),
                "statistics": {
                    "datasets": {
                        "total_datasets": 0,
                    },
                    "jobs": {
                        "total_jobs": 0,
                    },
                },
            },
        ],
    }


async def test_search_locations_no_results(
    test_client: AsyncClient,
    locations_search: tuple[dict[str, Location], dict[str, Location]],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/locations",
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
