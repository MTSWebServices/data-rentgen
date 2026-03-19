from collections import defaultdict
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json, run_to_json
from tests.test_server.utils.enrich import enrich_runs
from tests.test_server.utils.lineage_result import LineageResult

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_runs_by_unknown_id(
    test_client: AsyncClient,
    new_run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"run_id": str(new_run.id)},
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


async def test_get_runs_by_one_id(
    test_client: AsyncClient,
    run: Run,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    [run] = await enrich_runs([run], async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"run_id": str(run.id)},
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


async def test_get_runs_by_multiple_ids(
    test_client: AsyncClient,
    runs: list[Run],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    # create more objects than pass to endpoint, to test filtering
    selected_runs = await enrich_runs(runs[:2], async_session)

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"run_id": [str(run.id) for run in selected_runs]},
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
            for run in sorted(selected_runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }


async def test_get_runs_by_multiple_ids_with_stats(
    test_client: AsyncClient,
    simple_lineage: LineageResult,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    runs = await enrich_runs(simple_lineage.runs, async_session)

    input_stats = defaultdict(dict)
    output_stats = defaultdict(dict)
    operation_stats = defaultdict(dict)
    for run in runs:
        input_bytes = 0
        input_rows = 0
        input_files = 0
        output_bytes = 0
        output_rows = 0
        output_files = 0

        for input in simple_lineage.inputs:
            if input.run_id != run.id:
                continue
            input_bytes += input.num_bytes or 0
            input_rows += input.num_rows or 0
            input_files += input.num_files or 0

        for output in simple_lineage.outputs:
            if output.run_id != run.id:
                continue
            output_bytes += output.num_bytes or 0
            output_rows += output.num_rows or 0
            output_files += output.num_files or 0

        operation_stats[run.id] = {
            "total_operations": 2,
        }

        input_stats[run.id] = {
            # each of 2 operation within run interacts with one dataset
            "total_datasets": 2,
            "total_bytes": input_bytes,
            "total_rows": input_rows,
            "total_files": input_files,
        }

        output_stats[run.id] = {
            # each of 2 operation within run interacts with one dataset
            "total_datasets": 2,
            "total_bytes": output_bytes,
            "total_rows": output_rows,
            "total_files": output_files,
        }

    response = await test_client.get(
        "v1/runs",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"run_id": [str(run.id) for run in runs]},
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
                    "inputs": input_stats[run.id],
                    "outputs": output_stats[run.id],
                    "operations": operation_stats[run.id],
                },
            }
            for run in sorted(runs, key=lambda x: (x.created_at, x.id), reverse=True)
        ],
    }
