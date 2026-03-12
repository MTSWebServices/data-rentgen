from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Dataset
from data_rentgen.db.models.output import OutputType
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.schema import create_schema
from tests.test_server.utils.convert_to_json import (
    datasets_to_json,
    format_datetime,
    inputs_to_json,
    jobs_to_json,
    location_to_json,
    operation_parents_to_json,
    operations_to_json,
    outputs_to_json,
    run_parents_to_json,
    runs_to_json,
    schema_to_json,
    symlinks_to_json,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_dataset_lineage_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/datasets/lineage")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_dataset_lineage_via_personal_token_is_allowed(
    test_client: AsyncClient,
    dataset: Dataset,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={
            "since": datetime.now(tz=UTC).isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()


async def test_get_dataset_lineage_no_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    dataset: Dataset,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": dataset.id,
        },
    )

    [dataset] = await enrich_datasets([dataset], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    job_ids = {input.job_id for input in inputs} | {output.job_id for output in outputs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "JOB",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs) + operation_parents_to_json(operations),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(inputs, granularity="OPERATION"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(outputs, granularity="OPERATION"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # We need a middle dataset, which has inputs and outputs
    lineage_dataset = lineage.datasets[1]
    # If start dataset is d1 we should have this lineage: d0-d1-d2
    datasets = lineage.datasets[:3]
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


@pytest.mark.parametrize(
    ["direction", "start_dataset_id"],
    [("DOWNSTREAM", 1), ("UPSTREAM", 3)],
    ids=["DOWNSTREAM", "UPSTREAM"],
)
async def test_get_dataset_lineage_with_granularity_dataset_and_direction(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
    direction: str,
    start_dataset_id,
):
    lineage = lineage_with_depth
    # We need a middle dataset, which has inputs and outputs
    lineage_dataset = lineage.datasets[start_dataset_id]
    # If start dataset is d1 we should have this lineage: d0-d1-d2
    datasets = lineage.datasets[1:4]
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
            "direction": direction,
            "depth": 2,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_and_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # We need a middle dataset, which has inputs and outputs
    lineage_dataset = lineage.datasets[1]
    # If start dataset is d1 and depth = 2 we should have this lineage: d0-d1-d2-d3-d4
    datasets = lineage.datasets
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_and_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks_dataset_granularity: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_symlinks_dataset_granularity
    # We need a middle symlink dataset, which has inputs and outputs
    # If start dataset is ds2 and depth = 2 we should have this lineage: d1->ds2 d2->ds3 d3 -> ds4
    datasets, symlink_datasets = lineage.datasets[:3], lineage.datasets[5:]
    lineage_dataset = symlink_datasets[0]
    dataset_pairs_ids = [(from_.id, to.id) for (from_, to) in zip(datasets, symlink_datasets, strict=False)]

    dataset_symlinks = lineage.dataset_symlinks
    inputs_by_dataset_id = {input_.dataset_id: input_ for input_ in lineage.inputs}
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    datasets = await enrich_datasets(lineage.datasets, async_session)

    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
            "depth": 2,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": symlinks_to_json(dataset_symlinks),
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(from_id)},
                        "to": {"kind": "DATASET", "id": str(to_id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[to_id].created_at),
                    }
                    for from_id, to_id in dataset_pairs_ids
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": (
                        schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH")
                        # symlinks without IO have no schema
                        if dataset.id in inputs_by_dataset_id or dataset.id in outputs_by_dataset_id
                        else None
                    ),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_and_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # We need a middle dataset, which has inputs and outputs
    lineage_dataset = lineage.datasets[1]
    # If start dataset is d1 and depth = 3 we should have this lineage: d0-d1-d2-d3-d4
    # If we add until of operation between d2 and d3 we will have this lineage: d0-d1-d2
    datasets = lineage.datasets[:3]
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    datasets = await enrich_datasets(datasets, async_session)
    since = min(operation.created_at for operation in lineage.operations)
    until = since + timedelta(seconds=1)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], inputs=inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    run_ids = {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "direction": "UPSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs=outputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    since = min([input.created_at for input in inputs] + [output.created_at for output in outputs])
    until = since + timedelta(seconds=0.1)

    inputs_with_until = [input for input in inputs if since <= input.created_at <= until]
    assert inputs_with_until

    outputs_with_until = [output for output in outputs if since <= output.created_at <= until]
    assert outputs_with_until

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1, D1 -*> R1 -*> D2
    # J2 -*> R2, D2 -*> R2 --> D3
    # J3 --> R3, D3 --> R3 --> D4

    # Go dataset -> runs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_run_ids = {input.run_id for input in first_level_inputs}
    first_level_output_run_ids = {output.run_id for output in first_level_outputs}
    first_level_run_ids = first_level_input_run_ids | first_level_output_run_ids
    assert first_level_run_ids

    # Go runs[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.run_id in first_level_output_run_ids]
    second_level_outputs = [output for output in lineage.outputs if output.run_id in first_level_input_run_ids]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids - {
        first_level_dataset.id,
    }
    assert second_level_dataset_ids

    # Go datasets[second level] -> runs[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_run_ids = {input.run_id for input in third_level_inputs}
    third_level_output_run_ids = {output.run_id for output in third_level_outputs}
    third_level_run_ids = third_level_input_run_ids | third_level_output_run_ids - first_level_run_ids
    assert third_level_run_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run_ids = first_level_run_ids | third_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_depth_and_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # D1 -*> J1 -*> D2
    # D2 -*> J2 --> D3
    # D3 --> J3 --> D4

    # Go dataset[first level] -> jobs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_job_ids = {input.job_id for input in first_level_inputs}
    first_level_output_job_ids = {output.job_id for output in first_level_outputs}
    first_level_job_ids = first_level_input_job_ids | first_level_output_job_ids
    assert first_level_job_ids

    # Go job[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.job_id in first_level_output_job_ids]
    second_level_outputs = [output for output in lineage.outputs if output.job_id in first_level_input_job_ids]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids
    assert second_level_dataset_ids

    # Go datasets[second level] -> jobs[third level]
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_job_ids = {input.job_id for input in third_level_inputs}
    third_level_output_job_ids = {output.job_id for output in third_level_outputs}
    third_level_job_ids = third_level_input_job_ids | third_level_output_job_ids - first_level_job_ids
    assert third_level_job_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    job_ids = first_level_job_ids | third_level_job_ids
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id in job_ids)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
            "granularity": "JOB",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_depth_and_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 --> D3
    # J3 --> R3 --> O3, D3 --> O3 --> D4

    # Go datasets[first level] -> operations[first level] + runs[first level] + jobs[first level]
    first_level_dataset = lineage.datasets[0]
    first_level_inputs = [input for input in lineage.inputs if input.dataset_id == first_level_dataset.id]
    first_level_outputs = [output for output in lineage.outputs if output.dataset_id == first_level_dataset.id]
    first_level_input_operation_ids = {input.operation_id for input in first_level_inputs}
    first_level_output_operation_ids = {output.operation_id for output in first_level_outputs}
    first_level_operation_ids = first_level_input_operation_ids | first_level_output_operation_ids
    assert first_level_operation_ids

    # Go operations[first level] -> datasets[second level]
    second_level_inputs = [input for input in lineage.inputs if input.operation_id in first_level_output_operation_ids]
    second_level_outputs = [
        output for output in lineage.outputs if output.operation_id in first_level_input_operation_ids
    ]
    second_level_input_dataset_ids = {input.dataset_id for input in second_level_inputs}
    second_level_output_dataset_ids = {output.dataset_id for output in second_level_outputs}
    second_level_dataset_ids = second_level_input_dataset_ids | second_level_output_dataset_ids
    assert second_level_dataset_ids

    # Go datasets[second level] -> operations[third level] + runs[third level] + jobs[third level]
    third_level_inputs = [input for input in lineage.inputs if input.dataset_id in second_level_output_dataset_ids]
    third_level_outputs = [output for output in lineage.outputs if output.dataset_id in second_level_input_dataset_ids]
    third_level_input_operation_ids = {input.operation_id for input in third_level_inputs}
    third_level_output_operation_ids = {output.operation_id for output in third_level_outputs}
    third_level_operation_ids = (
        third_level_input_operation_ids | third_level_output_operation_ids - first_level_operation_ids
    )
    assert third_level_operation_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = {first_level_dataset.id} | second_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    operation_ids = first_level_operation_ids | third_level_operation_ids
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {input.run_id for input in inputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {input.job_id for input in inputs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    runs = await enrich_runs(runs, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_dataset.id,
            "depth": 3,
            "granularity": "OPERATION",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs) + operation_parents_to_json(operations),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(inputs, granularity="OPERATION"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(outputs, granularity="OPERATION"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
    }


async def test_get_dataset_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    cyclic_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = cyclic_lineage
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any dataset
    dataset = lineage.datasets[0]

    datasets = await enrich_datasets(lineage.datasets, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(lineage.inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(lineage.outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(lineage.outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, lineage.outputs, lineage.inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_depth_ignore_unrelated_datasets(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage
    # Start from D3, build lineage with direction=BOTH
    initial_dataset = lineage.datasets[3]

    # Select only relations marked with *
    #     D0   D1
    #      *\ /*
    # J0 -*> R0 -> D2
    #        *\
    #         D3  D4
    #          *\ /
    #     J1 -*> R1 -*> D5
    #            *\
    #             D6  D7
    #              *\ /
    #         J2 -*> R2 -> D8
    #                 \
    #                  D9

    datasets = [
        lineage.datasets[0],
        lineage.datasets[1],
        # D2 is not a part of (D3,R1,D0,D1) input chain
        lineage.datasets[3],
        # D4 is not a part of (D3,R1,D0,D1) input chain
        lineage.datasets[5],
        lineage.datasets[6],
        # D7 is not a part of (D3,R1,D6) input chain
        # D8, D9 exceed depth=3
    ]
    dataset_ids = {dataset.id for dataset in datasets}

    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    assert outputs

    jobs = await enrich_jobs(lineage.jobs, async_session)
    runs = await enrich_runs(lineage.runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_symlink(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_symlinks
    # Start from dataset between J0 and J1 lineages
    initial_dataset = lineage.datasets[1]

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if initial_dataset.id in (dataset_symlink.from_dataset_id, dataset_symlink.to_dataset_id)
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids = {initial_dataset.id} | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    # Threat all datasets from symlinks like they were passed as `start_node_id`
    inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    assert outputs

    operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": initial_dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": symlinks_to_json(dataset_symlinks),
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_symlink_without_input_output(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_unconnected_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_unconnected_symlinks
    # Start from any dataset between J0 and J1, as it has both inputs and outputs
    dataset = lineage.datasets[1]

    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    assert outputs

    operation_ids = {input.operation_id for input in inputs} | {output.operation_id for output in outputs}
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    dataset_ids = {dataset.id}

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_include_symlinks = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids_include_symlinks]
    assert datasets

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": symlinks_to_json(dataset_symlinks),
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


@pytest.mark.parametrize("dataset_index", [0, 1], ids=["output", "input"])
async def test_get_dataset_lineage_unmergeable_schema_and_output_type(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    dataset_index: int,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # make every input and output a different schema -> grouping by Run returns None.
    # make every output a different type -> grouping by Run returns None.
    response_schema = None
    for raw_input in lineage.inputs:
        schema = await create_schema(async_session)
        raw_input.schema_id = schema.id
        raw_input.schema = schema
        await async_session.merge(raw_input)
    response_schema = schema if dataset_index == 0 else response_schema

    output_types = list(OutputType)
    for i, raw_output in enumerate(lineage.outputs):
        schema = await create_schema(async_session)
        raw_output.schema_id = schema.id
        raw_output.schema = schema
        raw_output.type = output_types[i % len(output_types)]
        await async_session.merge(raw_output)
    response_schema = schema if dataset_index == 1 else response_schema

    await async_session.commit()

    # one dataset has only inputs, another only outputs - test both
    dataset = lineage.datasets[dataset_index]
    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(response_schema, "LATEST_KNOWN"),
                },
            },
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


@pytest.mark.parametrize("dataset_index", [0, 1], ids=["output", "input"])
async def test_get_dataset_lineage_empty_io_stats_and_schema(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    dataset_index: int,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # clear input/output stats and schema.
    for raw_input in lineage.inputs:
        raw_input.schema_id = None
        raw_input.schema = None
        raw_input.num_bytes = None
        raw_input.num_rows = None
        raw_input.num_files = None
        await async_session.merge(raw_input)

    for raw_output in lineage.outputs:
        raw_output.schema_id = None
        raw_output.schema = None
        raw_output.num_bytes = None
        raw_output.num_rows = None
        raw_output.num_files = None
        await async_session.merge(raw_output)

    await async_session.commit()

    # one dataset has only inputs, another only outputs - test both
    dataset = lineage.datasets[dataset_index]
    inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    [dataset] = await enrich_datasets([dataset], async_session)

    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    # merge_io_by_runs sums empty bytes, rows and files, producing 0 instead of None.
    # override that
    merged_run_inputs = merge_io_by_runs(inputs)
    for input in merged_run_inputs:
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None

    merged_run_outputs = merge_io_by_runs(outputs)
    for output in merged_run_outputs:
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None

    merged_job_inputs = merge_io_by_jobs(inputs)
    for input in merged_job_inputs:
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None

    merged_job_outputs = merge_io_by_jobs(outputs)
    for output in merged_job_outputs:
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merged_job_inputs, granularity="JOB"),
                *inputs_to_json(merged_run_inputs, granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merged_job_outputs, granularity="JOB"),
                *outputs_to_json(merged_run_outputs, granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset]),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_without_output_schema(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth

    # delete output schema for operation 01
    output = lineage.outputs[0]
    output.schema_id = None
    output.schema = None
    await async_session.merge(output)
    await async_session.commit()

    response_schema = await create_schema(async_session)
    # Add new schema to O2 input. It should be different from all others
    input_ = lineage.inputs[1]
    input_.schema_id = response_schema.id
    input_.schema = response_schema
    await async_session.merge(input_)
    await async_session.commit()

    # dasate which has only input schema
    lineage_dataset = lineage.datasets[1]
    datasets = lineage.datasets[:3]
    outputs_by_dataset_id = {output.dataset_id: output for output in lineage.outputs}

    runs = await enrich_runs(lineage.runs, async_session)
    [lineage_dataset] = await enrich_datasets([lineage_dataset], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    since = min(run.created_at for run in runs)
    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "outputs": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(lineage_dataset.id): {
                    "id": str(lineage_dataset.id),
                    "name": lineage_dataset.name,
                    "location": location_to_json(lineage_dataset.location),
                    "schema": schema_to_json(response_schema, "EXACT_MATCH"),
                },
                str(datasets[0].id): {
                    "id": str(datasets[0].id),
                    "name": datasets[0].name,
                    "location": location_to_json(datasets[0].location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                },
                str(datasets[2].id): {
                    "id": str(datasets[2].id),
                    "name": datasets[2].name,
                    "location": location_to_json(datasets[2].location),
                    "schema": schema_to_json(lineage.inputs[0].schema, "EXACT_MATCH"),
                },
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_ignore_self_references(
    test_client: AsyncClient,
    async_session: AsyncSession,
    self_referencing_lineage: LineageResult,
    mocked_user: MockedUser,
):
    # For this lineage:
    # J1 -> R1 -> O1, D1 -> O1 -> D1  # reading duplicates and removing them
    lineage = self_referencing_lineage

    # We start at D1
    [dataset] = await enrich_datasets(lineage.datasets[:1], async_session)

    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "DATASET",
        },
    )

    # And return no lineage, only dataset itself
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": None,
                },
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


# TODO: handle this case in other granularities
async def test_get_dataset_lineage_with_granularity_dataset_ignore_not_connected_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_non_connected_operations: LineageResult,
    mocked_user: MockedUser,
):
    # For this lineage:
    # J1 -> R1 -> O1, D1 -> O1        # SELECT max(id) FROM table1
    # J1 -> R1 -> O2,       O2 -> D2  # INSERT INTO table1 VALUES
    lineage = lineage_with_non_connected_operations

    # We start at D1
    [dataset] = await enrich_datasets(lineage.datasets[:1], async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": dataset.id,
            "granularity": "DATASET",
        },
    )

    # Return only nothing
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": None,
                },
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_for_long_running_operations_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_for_long_running_operations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_for_long_running_operations

    # We need a middle dataset, which has inputs and outputs
    dataset = lineage.datasets[1]

    # use only latest IO for each operation+dataset
    raw_inputs = [input for input in lineage.inputs if input.dataset_id == dataset.id]
    latest_inputs = {}
    for input in raw_inputs:
        index = (input.operation_id, input.dataset_id)
        existing = latest_inputs.get(index)
        if not existing or input.created_at > existing.created_at:
            latest_inputs[index] = input
    inputs = list(latest_inputs.values())
    assert inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id == dataset.id]
    latest_outputs = {}
    for output in raw_outputs:
        index = (output.operation_id, input.dataset_id)
        existing = latest_outputs.get(index)
        if not existing or output.created_at > existing.created_at:
            latest_outputs[index] = output
    outputs = list(latest_outputs.values())
    assert outputs

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    [dataset] = await enrich_datasets([dataset], async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": runs[0].created_at.isoformat(),
            "start_node_id": dataset.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json([dataset], outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_for_long_running_operations_with_granularity_dataset(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_for_long_running_operations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_for_long_running_operations

    # We need a middle dataset, which has inputs and outputs
    lineage_dataset = lineage.datasets[1]
    # Include D1 -> D2 -> D3
    datasets = lineage.datasets[:3]

    dataset_ids = {dataset.id for dataset in datasets}

    # use only latest IO for each operation+dataset
    raw_inputs = [input for input in lineage.inputs if input.dataset_id in dataset_ids]
    latest_inputs = {}
    for input in raw_inputs:
        index = (input.operation_id, input.dataset_id)
        existing = latest_inputs.get(index)
        if not existing or input.created_at > existing.created_at:
            latest_inputs[index] = input
    inputs = list(latest_inputs.values())
    assert inputs

    raw_outputs = [output for output in lineage.outputs if output.dataset_id in dataset_ids]
    latest_outputs = {}
    for output in raw_outputs:
        index = (output.operation_id, input.dataset_id)
        existing = latest_outputs.get(index)
        if not existing or output.created_at > existing.created_at:
            latest_outputs[index] = output
    outputs = list(latest_outputs.values())
    assert outputs

    outputs_by_dataset_id = {output.dataset_id: output for output in outputs}

    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    datasets = await enrich_datasets(datasets, async_session)
    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)

    response = await test_client.get(
        "v1/datasets/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": runs[0].created_at.isoformat(),
            "start_node_id": lineage_dataset.id,
            "granularity": "DATASET",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": sorted(
                [
                    {
                        "from": {"kind": "DATASET", "id": str(datasets[i].id)},
                        "to": {"kind": "DATASET", "id": str(datasets[i + 1].id)},
                        "num_bytes": None,
                        "num_rows": None,
                        "num_files": None,
                        "last_interaction_at": format_datetime(outputs_by_dataset_id[datasets[i + 1].id].created_at),
                    }
                    for i in range(len(datasets) - 1)
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }
