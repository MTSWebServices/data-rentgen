from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, run_to_json
from tests.test_server.utils.enrich import enrich_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_parent_run_id_missing_since(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "parent_run_id": str(new_run.parent_run_id),
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
                    "message": "Value error, 'parent_run_id' can be passed only with 'since'",
                    "context": {},
                    "input": {
                        "job_id": [],
                        "job_location_id": [],
                        "job_type": [],
                        "page_size": 20,
                        "page": 1,
                        "parent_run_id": [str(new_run.parent_run_id)],
                        "run_id": [],
                        "started_by_user": [],
                        "status": [],
                    },
                },
            ],
        },
    }


async def test_get_runs_by_parent_run_id_unknown(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
) -> None:
    since = new_run.created_at

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "parent_run_id": str(new_run.parent_run_id),
        },
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


async def test_get_runs_by_parent_run_id(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_with_same_parent: list[Run],
    mocked_user: MockedUser,
) -> None:
    since = min(run.created_at for run in runs_with_same_parent)
    runs = await enrich_runs(runs_with_same_parent, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "parent_run_id": str(runs_with_same_parent[0].parent_run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 5,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
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
            for run in sorted(runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }


async def test_get_runs_by_parent_run_id_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_with_same_parent: list[Run],
    mocked_user: MockedUser,
) -> None:
    since = min(run.created_at for run in runs_with_same_parent)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs_with_same_parent if since <= run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "parent_run_id": str(runs_with_same_parent[0].parent_run_id),
        },
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
            for run in sorted(runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }
