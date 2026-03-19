from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, run_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_jobs, enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_no_filters(
    test_client: AsyncClient,
    jobs: list[Job],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    jobs = await enrich_jobs(jobs, async_session)
    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(jobs),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(job.id),
                "data": job_to_json(job),
                "tags": tag_values_to_json(job.tag_values) if job.tag_values else [],
                "last_run": None,
            }
            for job in sorted(jobs, key=lambda x: x.name)
        ],
    }


async def test_get_jobs_with_last_run(
    test_client: AsyncClient,
    runs_with_same_job: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    last_run = max(runs_with_same_job, key=lambda x: x.created_at)
    [last_run] = await enrich_runs([last_run], async_session)
    [job] = await enrich_jobs([last_run.job], async_session)
    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(job.id),
                "data": job_to_json(job),
                "tags": tag_values_to_json(job.tag_values) if job.tag_values else [],
                "last_run": run_to_json(last_run),
            }
        ],
    }


async def test_get_jobs_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/jobs")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_jobs_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
