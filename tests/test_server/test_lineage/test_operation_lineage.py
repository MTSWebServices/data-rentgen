from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, Operation, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import (
    datasets_to_json,
    inputs_to_json,
    jobs_to_json,
    operations_to_json,
    outputs_to_json,
    run_parents_to_json,
    runs_to_json,
    symlinks_to_json,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_operation_lineage_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/operations/lineage")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_operation_lineage_via_personal_token_is_allowed(
    test_client: AsyncClient,
    operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={
            "since": operation.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()


async def test_get_operation_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    operation: Operation,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)

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
            "datasets": {},
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_simple(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)
    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(outputs, granularity="OPERATION"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs=outputs),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
            "direction": "UPSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(inputs, granularity="OPERATION"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, inputs=inputs),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    # TODO: This test should be change cause `until` for operation has sense only for `depth` > 1
    lineage = simple_lineage
    operation = lineage.operations[0]

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    # Operations within same run are 1 second apart
    since = operation.created_at
    until = since + timedelta(seconds=1)

    inputs = [
        input for input in lineage.inputs if input.operation_id == operation.id and since <= input.created_at <= until
    ]
    assert inputs

    outputs = [
        output
        for output in lineage.outputs
        if output.operation_id == operation.id and since <= output.created_at <= until
    ]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D3
    # J3 --> R3 --> O3, D3 --> O3 --> D4

    first_level_operation = lineage.operations[0]

    # Go operations[first level] -> datasets[second level]
    first_level_inputs = [input for input in lineage.inputs if input.operation_id == first_level_operation.id]
    first_level_outputs = [output for output in lineage.outputs if output.operation_id == first_level_operation.id]
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[second level] -> operations[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_operation_ids = {input.operation_id for input in second_level_inputs}
    second_level_output_operation_ids = {output.operation_id for output in second_level_outputs}
    second_level_operation_ids = second_level_input_operation_ids | second_level_output_operation_ids - {
        first_level_operation.id,
    }
    assert second_level_operation_ids

    # Go operations[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.operation_id in second_level_output_operation_ids]
    third_level_outputs = [
        output for output in lineage.outputs if output.operation_id in second_level_input_operation_ids
    ]
    third_level_input_dataset_ids = {input.dataset_id for input in third_level_inputs}
    third_level_output_dataset_ids = {output.dataset_id for output in third_level_outputs}
    third_level_dataset_ids = third_level_input_dataset_ids | third_level_output_dataset_ids - first_level_dataset_ids
    assert third_level_dataset_ids

    inputs = first_level_inputs + second_level_inputs + third_level_inputs
    assert inputs

    outputs = first_level_outputs + second_level_outputs + third_level_outputs
    assert outputs

    dataset_ids = first_level_dataset_ids | third_level_dataset_ids
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    operation_ids = {first_level_operation.id} | second_level_operation_ids
    operations = [operation for operation in lineage.operations if operation.id in operation_ids]
    assert operations

    run_ids = {operation.run_id for operation in operations}
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
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": str(first_level_operation.id),
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
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


async def test_get_operation_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    cyclic_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = cyclic_lineage
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any operation
    operation = lineage.operations[0]

    runs = await enrich_runs(lineage.runs, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": str(operation.id),
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
                *inputs_to_json(lineage.inputs, granularity="OPERATION"),
                *inputs_to_json(merge_io_by_runs(lineage.inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(lineage.outputs), granularity="JOB"),
                *outputs_to_json(lineage.outputs, granularity="OPERATION"),
                *outputs_to_json(merge_io_by_runs(lineage.outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, lineage.outputs, lineage.inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(lineage.operations),
        },
    }


async def test_get_operation_lineage_with_depth_ignore_unrelated_datasets(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage
    # Start from O1, build lineage with direction=BOTH
    operation = lineage.operations[1]

    # Select only relations marked with *
    #            D0   D1
    #             *\ /*
    # J0 -*> R0 -*> O0 -> D2
    #               *\
    #                D3  D4
    #                 *\ /*
    #     J1 -*> R1 -*> O1 -*> D5
    #                   *\
    #                    D6  D7
    #                     *\ /
    #         J2 -*> R2 -*> O1 -*> D8
    #                       *\
    #                         D9

    datasets = [
        lineage.datasets[0],
        lineage.datasets[1],
        # D2 is not a part of (O1,D3,O0,D0,D1) input chain
        lineage.datasets[3],
        lineage.datasets[4],
        lineage.datasets[5],
        lineage.datasets[6],
        # D7 is not a part of (O1,D6,O2,D8,D9) output chain
        lineage.datasets[8],
        lineage.datasets[9],
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
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": str(operation.id),
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
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
            "operations": operations_to_json(lineage.operations),
        },
    }


async def test_get_operation_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_symlinks
    operation = lineage.operations[0]

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}

    dataset_symlinks = [
        dataset_symlink
        for dataset_symlink in lineage.dataset_symlinks
        if dataset_symlink.from_dataset_id in dataset_ids or dataset_symlink.to_dataset_id in dataset_ids
    ]
    dataset_ids_from_symlink = {dataset_symlink.from_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_to_symlink = {dataset_symlink.to_dataset_id for dataset_symlink in dataset_symlinks}
    dataset_ids_with_symlinks = dataset_ids | dataset_ids_from_symlink | dataset_ids_to_symlink
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids_with_symlinks]
    assert datasets

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": symlinks_to_json(dataset_symlinks),
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_symlink_without_input_output(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_unconnected_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_unconnected_symlinks
    operation = lineage.operations[0]

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    assert dataset_ids

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

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": symlinks_to_json(dataset_symlinks),
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_empty_io_stats_and_schema(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage

    # clear input/output stats
    for input in lineage.inputs:
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None
        input.schema_id = None
        input.schema = None
        await async_session.merge(input)

    for output in lineage.outputs:
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None
        output.schema_id = None
        output.schema = None
        await async_session.merge(output)

    await async_session.commit()

    operation = lineage.operations[0]

    inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    datasets = await enrich_datasets(datasets, async_session)
    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
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
            "parents": [],
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merged_job_inputs, granularity="JOB"),
                *inputs_to_json(inputs, granularity="OPERATION"),
                *inputs_to_json(merged_run_inputs, granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merged_job_outputs, granularity="JOB"),
                *outputs_to_json(outputs, granularity="OPERATION"),
                *outputs_to_json(merged_run_outputs, granularity="RUN"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_for_long_running_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_for_long_running_operations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_for_long_running_operations

    operation = lineage.operations[0]

    # use only latest IO for each operation+dataset
    raw_inputs = [input for input in lineage.inputs if input.operation_id == operation.id]
    latest_inputs = {}
    for input in raw_inputs:
        index = (input.operation_id, input.dataset_id)
        existing = latest_inputs.get(index)
        if not existing or input.created_at > existing.created_at:
            latest_inputs[index] = input
    inputs = list(latest_inputs.values())
    assert inputs

    raw_outputs = [output for output in lineage.outputs if output.operation_id == operation.id]
    latest_outputs = {}
    for output in raw_outputs:
        index = (output.operation_id, input.dataset_id)
        existing = latest_outputs.get(index)
        if not existing or output.created_at > existing.created_at:
            latest_outputs[index] = output
    outputs = list(latest_outputs.values())
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    run = next(run for run in lineage.runs if run.id == operation.run_id)
    job = next(job for job in lineage.jobs if job.id == run.job_id)

    datasets = await enrich_datasets(datasets, async_session)
    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_operation_lineage_with_run_parents_chain(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_runs_chain: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_runs_chain
    operation = lineage.operations[0]
    run = next(run for run in lineage.runs if run.id == operation.run_id)
    since = run.created_at

    job = next(job for job in lineage.jobs if job.id == run.job_id)
    jobs = await enrich_jobs([job], async_session)
    lineage.runs.pop(-2)
    runs = await enrich_runs(lineage.runs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)

    response = await test_client.get(
        "v1/operations/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": str(operation.id),
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
                *inputs_to_json(lineage.inputs, granularity="OPERATION"),
                *inputs_to_json(merge_io_by_runs(lineage.inputs), granularity="RUN"),
            ],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(lineage.operations),
        },
    }
