from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
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


async def test_get_runs_by_parent_run_id_unknown(
    test_client: AsyncClient,
    new_run: Run,
    runs_with_same_parent: list[Run],
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
    runs = await enrich_runs(runs_with_same_parent, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "parent_run_id": str(runs_with_same_parent[0].parent_run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(runs),
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
                "statistics": EMPTY_STATS,
            }
            for run in sorted(runs, key=lambda x: (x.id, x.created_at), reverse=True)
        ],
    }


async def test_get_runs_by_parent_run_id_with_since(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_with_same_parent: list[Run],
    mocked_user: MockedUser,
) -> None:
    since = min(run.created_at for run in runs_with_same_parent) + timedelta(seconds=1)

    selected_runs = [run for run in runs_with_same_parent if since <= run.created_at]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "parent_run_id": str(runs[0].parent_run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(runs),
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
                "statistics": EMPTY_STATS,
            }
            for run in sorted(runs, key=lambda x: (x.id, x.created_at), reverse=True)
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

    selected_runs = [run for run in runs_with_same_parent if run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "until": until.isoformat(),
            "parent_run_id": str(runs[0].parent_run_id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(runs),
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
                "statistics": EMPTY_STATS,
            }
            for run in sorted(runs, key=lambda x: (x.id, x.created_at), reverse=True)
        ],
    }
