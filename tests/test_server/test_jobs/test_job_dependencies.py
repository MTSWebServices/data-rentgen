# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import jobs_ancestors_to_json, jobs_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_dependencies_nonexistent_start_node(
    test_client: AsyncClient,
    new_job: Job,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/dependencies",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": new_job.id},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "dependencies": [],
        },
        "nodes": {"jobs": {}},
    }


async def test_get_job_dependencies_isolated_job(
    test_client: AsyncClient,
    job: Job,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    [job] = await enrich_jobs([job], async_session)

    response = await test_client.get(
        "v1/jobs/dependencies",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": job.id},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "dependencies": [],
        },
        "nodes": {"jobs": jobs_to_json([job])},
    }


async def test_get_job_dependencies_unauthorized(
    test_client: AsyncClient,
    job: Job,
):
    response = await test_client.get(
        "v1/jobs/dependencies",
        params={"start_node_id": job.id},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_job_dependencies_with_direction_both(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (
        (_, dag2, _),
        (task1, task2, task3),
        (_, spark2, _),
    ) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [
            dag2,
            task1,
            task2,
            task3,
            spark2,
        ],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/dependencies",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json([task2, spark2]),
                "dependencies": [
                    {
                        "from": {"kind": "JOB", "id": str(from_id)},
                        "to": {"kind": "JOB", "id": str(to_id)},
                        "type": type_,
                    }
                    for from_id, to_id, type_ in sorted(
                        [
                            (task1.id, task2.id, "DIRECT_DEPENDENCY"),
                            (task2.id, task3.id, "DIRECT_DEPENDENCY"),
                        ]
                    )
                ],
            },
            "nodes": {"jobs": jobs_to_json(expected_nodes)},
        }


async def test_get_job_dependencies_with_direction_upstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (_, dag2, _), (task1, task2, _), (_, spark2, _) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [task1, dag2, task2, spark2],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/dependencies",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id, "direction": "UPSTREAM"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json([task2, spark2]),
                "dependencies": [
                    {
                        "from": {"kind": "JOB", "id": str(task1.id)},
                        "to": {"kind": "JOB", "id": str(task2.id)},
                        "type": "DIRECT_DEPENDENCY",
                    },
                ],
            },
            "nodes": {"jobs": jobs_to_json(expected_nodes)},
        }


async def test_get_job_dependencies_with_direction_downstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (_, dag2, _), (_, task2, task3), (_, spark2, _) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [dag2, task2, spark2, task3],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/dependencies",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id, "direction": "DOWNSTREAM"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json([task2, spark2]),
                "dependencies": [
                    {
                        "from": {"kind": "JOB", "id": str(task2.id)},
                        "to": {"kind": "JOB", "id": str(task3.id)},
                        "type": "DIRECT_DEPENDENCY",
                    },
                ],
            },
            "nodes": {"jobs": jobs_to_json(expected_nodes)},
        }
