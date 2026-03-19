from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, JobType
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_types(
    test_client: AsyncClient,
    job_types: list[JobType],
    mocked_user: MockedUser,
) -> None:
    unique_job_type = {item.type for item in job_types}
    response = await test_client.get(
        "/v1/jobs/types",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"job_types": sorted(unique_job_type)}


async def test_get_jobs_by_job_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
) -> None:
    jobs = await enrich_jobs(jobs_with_locations_and_types, async_session)
    [_, dag_job, task_job] = jobs

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"job_type": ["AIRFLOW_DAG", "AIRFLOW_TASK"]},
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
            for job in (dag_job, task_job)
        ],
    }

    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_type": ["AIRFLOW_DAG", "AIRFLOW_TASK"],
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


async def test_get_jobs_by_non_existent_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    jobs_with_locations_and_types: tuple[Job, ...],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"job_type": "NO_EXISTENT_TYPE"},
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
