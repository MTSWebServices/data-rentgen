from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, run_to_json
from tests.test_server.utils.enrich import enrich_runs

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
    "operations": {
        "total_operations": 0,
    },
}


async def test_search_runs_by_external_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    runs = await enrich_runs(
        [
            # runs sorted by id in descending order
            runs_search["application_1638922609021_0002"],
            runs_search["application_1638922609021_0001"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            # search by word prefix
            "search_query": "1638922",
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
            "total_count": 2,
        },
        "items": [
            {
                "id": str(run.id),
                "data": run_to_json(run),
                "job": job_to_json(run.job),
                "statistics": EMPTY_STATS,
            }
            for run in runs
        ],
    }


async def test_search_runs_by_job_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    [run] = await enrich_runs(
        [
            runs_search["dag_0001"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "search_query": "airflow_dag",
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
                "id": str(run.id),
                "data": run_to_json(run),
                "job": job_to_json(run.job),
                "statistics": EMPTY_STATS,
            },
        ],
    }


async def test_search_runs_no_results(
    test_client: AsyncClient,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "search_query": "not-found",
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
            "total_count": 0,
        },
        "items": [],
    }
