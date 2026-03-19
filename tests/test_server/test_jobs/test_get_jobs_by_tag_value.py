from collections.abc import Callable
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import TagValue
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, tag_values_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_jobs_by_one_id_with_tags(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    make_job: Callable,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    _unused = await make_job()
    job = await make_job(tag_values=tag_values)
    [job] = await enrich_jobs([job], async_session)

    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"job_id": job.id},
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
                "tags": tag_values_to_json(tag_values),
                "last_run": None,
            },
        ],
    }


async def test_get_jobs_by_tag_value_id(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    make_job: Callable,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    tv1, *_ = tag_values
    _unused = await make_job()
    job = await make_job(tag_values=tag_values)
    [job] = await enrich_jobs([job], async_session)

    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_value_id": tv1.id},
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
                "last_run": None,
            },
        ],
    }


@pytest.mark.asyncio
async def test_get_jobs_by_multiple_tag_value_ids(
    test_client: AsyncClient,
    tag_values: list[TagValue],
    make_job: Callable,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    tv1, tv2, _tv3 = tag_values
    wanted_job = await make_job(tag_values=tag_values)
    [wanted_job] = await enrich_jobs([wanted_job], async_session)
    await make_job(tag_values=[tv1])
    await make_job()

    response = await test_client.get(
        "v1/jobs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params=[("tag_value_id", tv1.id), ("tag_value_id", tv2.id)],
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
                "id": str(wanted_job.id),
                "data": job_to_json(wanted_job),
                "tags": tag_values_to_json(wanted_job.tag_values),
                "last_run": None,
            },
        ],
    }
