from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Location
from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.job import Job
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import location_to_json
from tests.test_server.utils.enrich import enrich_locations

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_locations_by_unknown_id(
    test_client: AsyncClient,
    new_location: Location,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": new_location.id},
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


async def test_get_locations_by_one_id(
    test_client: AsyncClient,
    location: Location,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    [location] = await enrich_locations([location], async_session)

    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": location.id},
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


async def test_get_locations_by_multiple_ids(
    test_client: AsyncClient,
    locations: list[Location],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_locations = await enrich_locations(locations[:2], async_session)

    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": [location.id for location in selected_locations]},
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
            for location in sorted(selected_locations, key=lambda x: x.name)
        ],
    }


async def test_get_locations_by_multiple_ids_with_stats(
    test_client: AsyncClient,
    datasets: list[Dataset],
    jobs: list[Job],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    dataset_location_ids = {dataset.location_id for dataset in datasets}
    job_location_ids = {job.location_id for job in jobs}
    location_ids = dataset_location_ids | job_location_ids

    locations = [Location(id=id) for id in location_ids]
    selected_locations = await enrich_locations(locations, async_session)

    response = await test_client.get(
        "v1/locations",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": list(location_ids)},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 10 + 5,
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
                        "total_datasets": int(location.id in dataset_location_ids),
                    },
                    "jobs": {
                        "total_jobs": int(location.id in job_location_ids),
                    },
                },
            }
            for location in sorted(selected_locations, key=lambda x: x.name)
        ],
    }
