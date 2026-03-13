# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import job_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_dependencies_unauthorized(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
):
    (_, job_middle, _) = job_dependency_chain[1]
    response = await test_client.get(
        "v1/jobs/dependencies",
        params={"start_node_id": job_middle.id},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_job_dependencies_default_request(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (
        (source_root, job_root, target_root),
        (source_middle, job_middle, target_middle),
        (source_leaf, job_leaf, target_leaf),
    ) = job_dependency_chain
    all_jobs = await enrich_jobs(
        [
            job_root,
            job_middle,
            job_leaf,
            source_root,
            target_root,
            source_middle,
            target_middle,
            source_leaf,
            target_leaf,
        ],
        async_session,
    )

    response = await test_client.get(
        "v1/jobs/dependencies",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": job_middle.id},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}}
                for from_id, to_id in sorted([(job_root.id, job_middle.id), (job_middle.id, job_leaf.id)])
            ],
            "dependencies": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}, "type": type_}
                for from_id, to_id, type_ in sorted(
                    [
                        (source_root.id, job_root.id, "DIRECT_DEPENDENCY"),
                        (job_root.id, target_root.id, "DIRECT_DEPENDENCY"),
                        (source_middle.id, job_middle.id, "DIRECT_DEPENDENCY"),
                        (job_middle.id, target_middle.id, "DIRECT_DEPENDENCY"),
                        (source_leaf.id, job_leaf.id, "DIRECT_DEPENDENCY"),
                        (job_leaf.id, target_leaf.id, "DIRECT_DEPENDENCY"),
                    ]
                )
            ],
        },
        "nodes": [job_to_json(job) for job in sorted(all_jobs, key=lambda j: j.id)],
    }


async def test_get_job_dependencies_with_direction_upstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (source_root, job_root, _), (source_middle, job_middle, _), (source_leaf, job_leaf, _) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [job_root, job_middle, job_leaf, source_root, source_middle, source_leaf],
        async_session,
    )

    response = await test_client.get(
        "v1/jobs/dependencies",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": job_middle.id, "direction": "UPSTREAM"},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}}
                for from_id, to_id in sorted([(job_root.id, job_middle.id), (job_middle.id, job_leaf.id)])
            ],
            "dependencies": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}, "type": type_}
                for from_id, to_id, type_ in sorted(
                    [
                        (source_root.id, job_root.id, "DIRECT_DEPENDENCY"),
                        (source_middle.id, job_middle.id, "DIRECT_DEPENDENCY"),
                        (source_leaf.id, job_leaf.id, "DIRECT_DEPENDENCY"),
                    ]
                )
            ],
        },
        "nodes": [job_to_json(job) for job in sorted(expected_nodes, key=lambda j: j.id)],
    }


async def test_get_job_dependencies_with_direction_downstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (_, job_root, target_root), (_, job_middle, target_middle), (_, job_leaf, target_leaf) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [job_root, job_middle, job_leaf, target_root, target_middle, target_leaf],
        async_session,
    )

    response = await test_client.get(
        "v1/jobs/dependencies",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": job_middle.id, "direction": "DOWNSTREAM"},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}}
                for from_id, to_id in sorted([(job_root.id, job_middle.id), (job_middle.id, job_leaf.id)])
            ],
            "dependencies": [
                {"from": {"kind": "JOB", "id": str(from_id)}, "to": {"kind": "JOB", "id": str(to_id)}, "type": type_}
                for from_id, to_id, type_ in sorted(
                    [
                        (job_root.id, target_root.id, "DIRECT_DEPENDENCY"),
                        (job_middle.id, target_middle.id, "DIRECT_DEPENDENCY"),
                        (job_leaf.id, target_leaf.id, "DIRECT_DEPENDENCY"),
                    ]
                )
            ],
        },
        "nodes": [job_to_json(job) for job in sorted(expected_nodes, key=lambda j: j.id)],
    }
