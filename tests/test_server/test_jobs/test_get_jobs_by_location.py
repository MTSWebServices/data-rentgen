from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_by_location_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
) -> None:
    jobs = await enrich_jobs(jobs_with_locations_and_types, async_session)

    # first job in jobs has a different location unlike two others
    [_, dag_job, task_job] = jobs

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": dag_job.location_id},
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
                "id": str(job.id),
                "data": job_to_json(job),
                "tags": [],
                "last_run": None,
            }
            for job in [dag_job, task_job]
        ],
    }

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "location_id": dag_job.location_id,
            # test multiple filters
            "search_query": "my-job_dag",
        },
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
                "id": str(dag_job.id),
                "data": job_to_json(dag_job),
                "tags": tag_values_to_json(dag_job.tag_values) if dag_job.tag_values else [],
                "last_run": None,
            },
        ],
    }


async def test_get_jobs_by_location_id_non_existent(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_id": -1},
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


async def test_get_jobs_by_location_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
) -> None:
    jobs = await enrich_jobs(jobs_with_locations_and_types, async_session)

    # first job in jobs has a different location unlike two others
    [_, dag_job, task_job] = jobs

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": "HTTP"},  # case-insensitive
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
                "id": str(job.id),
                "data": job_to_json(job),
                "tags": tag_values_to_json(job.tag_values) if job.tag_values else [],
                "last_run": None,
            }
            for job in [dag_job, task_job]
        ],
    }

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "location_type": ["http"],
            # test multiple filters
            "search_query": "my-job_dag",
        },
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
                "id": str(dag_job.id),
                "data": job_to_json(dag_job),
                "tags": tag_values_to_json(dag_job.tag_values) if dag_job.tag_values else [],
                "last_run": None,
            },
        ],
    }


async def test_get_jobs_by_location_type_non_existent(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"location_type": "non_existing_location_type"},
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
