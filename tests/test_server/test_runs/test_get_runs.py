from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

from data_rentgen.db.models.run import Run
from data_rentgen.utils.uuid import generate_new_uuid
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


async def test_get_runs_by_run_id_until_less_than_since(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    since = datetime.now(tz=timezone.utc)
    until = since - timedelta(days=1)
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "run_id": str(uuid7()),
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["query", "until"],
                    "code": "value_error",
                    "message": "Value error, 'since' should be less than 'until'",
                    "context": {},
                    "input": until.isoformat(),
                },
            ],
        },
    }


async def test_get_runs_no_filters(
    test_client: AsyncClient,
    runs: list[Run],
    mocked_user: MockedUser,
    async_session: AsyncSession,
):
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "page": 1,
            "page_size": 2,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 2,
            "total_count": len(runs),
            "pages_count": len(runs) // 2,
            "has_next": True,
            "has_previous": False,
            "next_page": 2,
            "previous_page": None,
        },
        "items": [
            {
                "id": str(run.id),
                "data": run_to_json(run),
                "job": job_to_json(run.job),
                "statistics": EMPTY_STATS,
            }
            for run in sorted(runs, key=lambda x: (x.id, x.created_at), reverse=True)[:2]
        ],
    }

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "page": 2,
            "page_size": 2,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 2,
            "page_size": 2,
            "total_count": len(runs),
            "pages_count": len(runs) // 2,
            "has_next": True,
            "has_previous": True,
            "next_page": 3,
            "previous_page": 1,
        },
        "items": [
            {
                "id": str(run.id),
                "data": run_to_json(run),
                "job": job_to_json(run.job),
                "statistics": EMPTY_STATS,
            }
            for run in sorted(runs, key=lambda x: (x.id, x.created_at), reverse=True)[2:4]
        ],
    }


async def test_get_runs_with_since(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    since = min(run.created_at for run in runs)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
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


async def test_get_runs_with_until(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    since = min(run.created_at for run in runs)
    until = since + timedelta(seconds=1)

    selected_runs = [run for run in runs if run.created_at <= until]
    runs = await enrich_runs(selected_runs, async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "until": until.isoformat(),
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


async def test_get_runs_with_job_type(
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
            "job_type": "SPARK_APPLICATION",
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
            "total_count": len(runs),
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

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_type": ["SPARK_APPLICATION"],
            # multiple filters
            "search_query": "0002",
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
            }
            for run in runs[:1]
        ],
    }


async def test_get_runs_with_status(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    runs = await enrich_runs(
        [
            # runs sorted by id in descending order
            runs_search["dag_0001"],
            runs_search["application_1638922609021_0002"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "status": ["SUCCEEDED", "STARTED"],
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
            "total_count": len(runs),
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

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "status": ["SUCCEEDED", "STARTED"],
            "search_query": "dag",
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
            }
            for run in runs[:1]
        ],
    }


async def test_get_runs_with_started_at(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    runs = await enrich_runs(
        [
            runs_search["application_1638922609021_0001"],
            runs_search["application_1638922609021_0002"],
            runs_search["dag_0001"],
            runs_search["task_0001"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "started_since": runs[1].started_at.isoformat(),
            "started_until": runs[2].started_at.isoformat(),
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
            # runs sorted by id in descending order
            for run in [runs[2], runs[1]]
        ],
    }


async def test_get_runs_with_ended_at(
    test_client: AsyncClient,
    async_session: AsyncSession,
    runs_search: dict[str, Run],
    mocked_user: MockedUser,
) -> None:
    runs = await enrich_runs(
        [
            runs_search["application_1638922609021_0001"],
            runs_search["application_1638922609021_0002"],
            runs_search["dag_0001"],
            runs_search["task_0001"],
        ],
        async_session,
    )

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "ended_since": runs[1].ended_at.isoformat(),
            "ended_until": runs[3].ended_at.isoformat(),
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
            # runs sorted by id in descending order
            for run in [runs[3], runs[1]]
        ],
    }


async def test_get_runs_with_location_id(
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
    job = runs[0].job

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_location_id": job.location_id,
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
            "total_count": len(runs),
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

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "job_location_id": [job.location_id],
            # test multiple filters
            "search_query": "0002",
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
            }
            for run in runs[:1]
        ],
    }


async def test_get_runs_with_started_by_user(
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
    started_by_user = runs[0].started_by_user

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "started_by_user": started_by_user.name,
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
            "total_count": len(runs),
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

    response = await test_client.get(
        "/v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "started_by_user": [started_by_user.name],
            # test multiple filters
            "search_query": "0002",
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
            }
            for run in runs[:1]
        ],
    }


async def test_get_runs_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/runs", params={"run_id": str(generate_new_uuid())})

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_runs_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={
            "since": datetime.now(tz=UTC).isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
