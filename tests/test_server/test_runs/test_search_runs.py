from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, run_to_json
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_runs_missing_since(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "search_query": new_run.external_id,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query"],
                    "code": "value_error",
                    "message": "Value error, 'search_query' can be passed only with 'since'",
                    "context": {},
                    "input": {
                        "job_id": [],
                        "job_location_id": [],
                        "job_type": [],
                        "page_size": 20,
                        "page": 1,
                        "parent_run_id": [],
                        "run_id": [],
                        "search_query": new_run.external_id,
                        "started_by_user": [],
                        "status": [],
                    },
                },
            ],
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
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
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
                "statistics": {
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
                },
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
            "since": run.created_at.isoformat(),
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
                "statistics": {
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
                },
            },
        ],
    }


async def test_search_runs_no_results(
    test_client: AsyncClient,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    since = min(run.created_at for run in runs_search.values())

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
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
