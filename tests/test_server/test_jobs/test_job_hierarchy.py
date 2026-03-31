# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import UTC, datetime, timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Input, Job, Output
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import format_datetime, jobs_ancestors_to_json, jobs_to_json
from tests.test_server.utils.enrich import enrich_jobs

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_job_hierarchy_nonexistent_start_node(
    test_client: AsyncClient,
    new_job: Job,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/hierarchy",
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


async def test_get_job_hierarchy_isolated_job(
    test_client: AsyncClient,
    job: Job,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    [job] = await enrich_jobs([job], async_session)

    response = await test_client.get(
        "v1/jobs/hierarchy",
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


async def test_get_job_hierarchy_unauthorized(
    test_client: AsyncClient,
    job: Job,
):
    response = await test_client.get(
        "v1/jobs/hierarchy",
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


async def test_get_job_hierarchy_with_direction_both(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (
        (dag1, dag2, dag3),
        (task1, task2, task3),
        (spark1, spark2, spark3),
    ) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [dag1, dag2, dag3, task1, task2, task3, spark1, spark2, spark3],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/hierarchy",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json(expected_nodes),
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


async def test_get_job_hierarchy_with_direction_upstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (
        (dag1, dag2, _),
        (task1, task2, _),
        (spark1, spark2, _),
    ) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [dag1, task1, spark1, dag2, task2, spark2],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/hierarchy",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id, "direction": "UPSTREAM"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json(expected_nodes),
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


async def test_get_job_hierarchy_with_direction_downstream(
    test_client: AsyncClient,
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    (
        (_, dag2, dag3),
        (_, task2, task3),
        (_, spark2, spark3),
    ) = job_dependency_chain
    expected_nodes = await enrich_jobs(
        [dag2, task2, spark2, dag3, task3, spark3],
        async_session,
    )

    for start_node in [dag2, task2, spark2]:
        response = await test_client.get(
            "v1/jobs/hierarchy",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"start_node_id": start_node.id, "direction": "DOWNSTREAM"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {
            "relations": {
                "parents": jobs_ancestors_to_json(expected_nodes),
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


@pytest.mark.parametrize(
    ["depth", "direction", "expected_dep_indices", "expected_job_indices"],
    [
        pytest.param(1, "DOWNSTREAM", [(2, 3)], [2, 3], id="depth_1-downstream"),
        pytest.param(2, "DOWNSTREAM", [(2, 3), (3, 4)], [2, 3, 4], id="depth_2-downstream"),
        pytest.param(1, "UPSTREAM", [(1, 2)], [1, 2], id="depth_1-upstream"),
        pytest.param(2, "UPSTREAM", [(0, 1), (1, 2)], [0, 1, 2], id="depth_2-upstream"),
        pytest.param(1, "BOTH", [(1, 2), (2, 3)], [1, 2, 3], id="depth_1-both"),
        pytest.param(
            2,
            "BOTH",
            [(0, 1), (1, 2), (2, 3), (3, 4)],
            [0, 1, 2, 3, 4],
            id="depth_2-both",
        ),
        pytest.param(
            5,
            "BOTH",
            [(0, 1), (1, 2), (2, 3), (3, 4)],
            [0, 1, 2, 3, 4],
            id="depth_5-both",
        ),
    ],
)
async def test_get_job_hierarchy_with_depth(
    test_client: AsyncClient,
    job_dependency_depth_chain: tuple[Job, ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
    depth: int,
    direction: str,
    expected_dep_indices: list[tuple[int, int]],
    expected_job_indices: list[int],
):
    """
    Fixture chain: job_0 → job_1 → job_2 → job_3 → job_4
    Start node is always job_2 (middle of the chain).
    """
    jobs = job_dependency_depth_chain
    start_job = jobs[2]

    expected_jobs = await enrich_jobs([jobs[i] for i in expected_job_indices], async_session)

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": start_job.id, "depth": depth, "direction": direction},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "dependencies": [
                {
                    "from": {"kind": "JOB", "id": str(jobs[i].id)},
                    "to": {"kind": "JOB", "id": str(jobs[j].id)},
                    "type": "DIRECT_DEPENDENCY",
                }
                for i, j in sorted(expected_dep_indices)
            ],
        },
        "nodes": {"jobs": jobs_to_json(expected_jobs)},
    }


@pytest.mark.parametrize(
    ["direction", "start_node_index"],
    [
        pytest.param("UPSTREAM", 0, id="upstream_boundary"),
        pytest.param("DOWNSTREAM", 4, id="downstream_boundary"),
    ],
)
async def test_get_job_hierarchy_with_depth_on_boundary(
    test_client: AsyncClient,
    job_dependency_depth_chain: tuple[Job, ...],
    async_session: AsyncSession,
    mocked_user: MockedUser,
    direction: str,
    start_node_index: int,
):
    """
    Fixture chain: job_0 → job_1 → job_2 → job_3 → job_4
    Start node is job_0 or job_4.
    """
    jobs = job_dependency_depth_chain
    start_job = jobs[start_node_index]

    [expected_job] = await enrich_jobs([start_job], async_session)

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"start_node_id": start_job.id, "depth": 2, "direction": direction},
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "dependencies": [],
        },
        "nodes": {"jobs": jobs_to_json([expected_job])},
    }


@pytest.mark.parametrize(
    ["direction", "depth", "start_node_idx", "expected_deps"],
    [
        pytest.param("UPSTREAM", 1, 1, [(0, 1, "INFERRED_FROM_LINEAGE")], id="inferred-upstream-depth-1"),
        pytest.param(
            "UPSTREAM",
            2,
            2,
            [
                (1, 2, "DIRECT_DEPENDENCY"),
                (0, 1, "INFERRED_FROM_LINEAGE"),
            ],
            id="inferred-upstream-depth-2",
        ),
        pytest.param("DOWNSTREAM", 1, 3, [(3, 4, "INFERRED_FROM_LINEAGE")], id="inferred-downstream-depth-1"),
        pytest.param(
            "DOWNSTREAM",
            2,
            2,
            [
                (2, 3, "DIRECT_DEPENDENCY"),
                (3, 4, "INFERRED_FROM_LINEAGE"),
            ],
            id="inferred-downstream-depth-2",
        ),
        pytest.param(
            "BOTH",
            2,
            2,
            [
                (1, 2, "DIRECT_DEPENDENCY"),
                (2, 3, "DIRECT_DEPENDENCY"),
                (3, 4, "INFERRED_FROM_LINEAGE"),
                (0, 1, "INFERRED_FROM_LINEAGE"),
            ],
            id="inferred-both-depth-2",
        ),
    ],
)
async def test_get_job_hierarchy_with_inferred_dependencies(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job_dependency_chain_with_lineage: tuple[tuple[Job, Job, Job, Job, Job], ...],
    mocked_user: MockedUser,
    direction: str,
    depth: int,
    start_node_idx: int,
    expected_deps: list[tuple[int, int, str]],
):
    dags, tasks, sparks = job_dependency_chain_with_lineage
    start_node = tasks[start_node_idx]

    expected_ids = set()
    for from_idx, to_idx, _ in expected_deps:
        expected_ids.add(from_idx)
        expected_ids.add(to_idx)
    expected_dags = [dags[idx] for idx in expected_ids]
    expected_tasks = [tasks[idx] for idx in expected_ids]
    expected_sparks = [sparks[idx] for idx in expected_ids]
    expected_nodes = await enrich_jobs(expected_dags + expected_tasks + expected_sparks, async_session)

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "start_node_id": start_node.id,
            "direction": direction,
            "depth": depth,
            "infer_from_lineage": True,
            "since": datetime.min.replace(tzinfo=UTC).isoformat(),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": jobs_ancestors_to_json(expected_nodes),
            "dependencies": [
                {
                    "from": {"kind": "JOB", "id": str(tasks[from_idx].id)},
                    "to": {"kind": "JOB", "id": str(tasks[to_idx].id)},
                    "type": dep_type,
                }
                for from_idx, to_idx, dep_type in expected_deps
                if dep_type == "DIRECT_DEPENDENCY"
            ]
            + [
                {
                    "from": {"kind": "JOB", "id": str(sparks[from_idx].id)},
                    "to": {"kind": "JOB", "id": str(sparks[to_idx].id)},
                    "type": dep_type,
                }
                for from_idx, to_idx, dep_type in expected_deps
                if dep_type == "INFERRED_FROM_LINEAGE"
            ],
        },
        "nodes": {"jobs": jobs_to_json(expected_nodes)},
    }


async def test_get_job_hierarchy_with_inferred_dependencies_with_since_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job_dependency_chain_with_lineage: tuple[tuple[Job, Job, Job, Job, Job], ...],
    mocked_user: MockedUser,
):
    dags, tasks, sparks = job_dependency_chain_with_lineage
    start_node = tasks[2]

    # Cover both inferred links connected to spark0 and spark4.
    edge_task_ids = [sparks[0].id, sparks[4].id]
    min_input_created_at = await async_session.scalar(
        select(func.min(Input.created_at)).where(Input.job_id.in_(edge_task_ids)),
    ) - timedelta(seconds=2)
    max_output_created_at = await async_session.scalar(
        select(func.max(Output.created_at)).where(Output.job_id.in_(edge_task_ids)),
    ) + timedelta(seconds=2)

    expected_nodes = await enrich_jobs([*dags[1:4], *tasks[1:4], *sparks[1:4]], async_session)
    expected_deps = [
        (1, 2, "DIRECT_DEPENDENCY"),
        (2, 3, "DIRECT_DEPENDENCY"),
    ]

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "start_node_id": start_node.id,
            "direction": "BOTH",
            "depth": 2,
            "infer_from_lineage": True,
            "since": max_output_created_at.isoformat(),
            "until": min_input_created_at.isoformat(),
        },
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": jobs_ancestors_to_json(expected_nodes),
            "dependencies": [
                {
                    "from": {"kind": "JOB", "id": str(tasks[from_idx].id)},
                    "to": {"kind": "JOB", "id": str(tasks[to_idx].id)},
                    "type": dep_type,
                }
                for from_idx, to_idx, dep_type in expected_deps
            ],
        },
        "nodes": {"jobs": jobs_to_json(expected_nodes)},
    }


async def test_get_job_hierarchy_with_inferred_dependencies_without_since(
    test_client: AsyncClient,
    job_dependency_chain_with_lineage: tuple[tuple[Job, Job, Job, Job, Job], ...],
    mocked_user: MockedUser,
):
    _, tasks, _ = job_dependency_chain_with_lineage
    start_node = tasks[2]

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "start_node_id": start_node.id,
            "direction": "BOTH",
            "depth": 2,
            "infer_from_lineage": True,
        },
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "value_error",
                    "context": {},
                    "input": None,
                    "location": [
                        "since",
                    ],
                    "message": "Value error, 'since' is mandatory when 'infer_from_lineage' is used",
                },
            ],
            "message": "Invalid request",
        },
    }


async def test_get_job_hierarchy_with_inferred_dependencies_since_less_then_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job_dependency_chain_with_lineage: tuple[tuple[Job, Job, Job, Job, Job], ...],
    mocked_user: MockedUser,
):
    _, tasks, sparks = job_dependency_chain_with_lineage
    start_node = tasks[2]

    edge_task_ids = [sparks[0].id, sparks[4].id]
    min_input_created_at = await async_session.scalar(
        select(func.min(Input.created_at)).where(Input.job_id.in_(edge_task_ids)),
    ) - timedelta(seconds=2)
    max_output_created_at = await async_session.scalar(
        select(func.max(Output.created_at)).where(Output.job_id.in_(edge_task_ids)),
    ) + timedelta(seconds=2)

    response = await test_client.get(
        "v1/jobs/hierarchy",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "start_node_id": start_node.id,
            "direction": "BOTH",
            "depth": 2,
            "infer_from_lineage": True,
            "since": min_input_created_at.isoformat(),
            "until": max_output_created_at.isoformat(),
        },
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "details": [
                {
                    "code": "value_error",
                    "context": {},
                    "input": format_datetime(max_output_created_at),
                    "location": [
                        "until",
                    ],
                    "message": "Value error, 'since' should be less than 'until'",
                },
            ],
            "message": "Invalid request",
        },
    }
