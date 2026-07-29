from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import location_to_json
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_locations_no_filters(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    locations = await enrich_locations(locations, async_session)
    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(locations),
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
            }
            for location in sorted(locations, key=lambda x: x.name)
        ],
    }


async def test_get_location_types(
    test_client: AsyncClient,
    locations: list[Location],
    mocked_user: MockedUser,
) -> None:
    unique_location_type = {item.type for item in locations}
    response = await test_client.get(
        "/v1/locations/types",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"location_types": sorted(unique_location_type)}


async def test_get_locations_with_type(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    location_type = locations[0].type
    matching_locations = [location for location in locations if location.type == location_type]
    matching_locations = await enrich_locations(matching_locations, async_session)

    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": location_type.upper()},  # case-insensitive
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(matching_locations),
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
            }
            for location in sorted(matching_locations, key=lambda x: x.name)
        ],
    }


async def test_get_locations_with_non_existent_type(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": "whatever"},
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


async def test_get_locations_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/locations")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_locations_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
