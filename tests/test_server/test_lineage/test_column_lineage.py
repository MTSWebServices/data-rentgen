from datetime import timedelta
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.fixtures.mocks import MockedUser
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
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_dataset_lineage_with_empty_column_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    """
    For dataset lineage with default depth = 1 response include only one dataset, so column lineage doesn't have sense.
    And direct_column_lineage and indirect_column_lineage should be empty.
    """
    lineage = duplicated_lineage_with_column_lineage
    dataset = lineage.datasets[0]

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
            "include_column_lineage": True,
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


async def test_get_operation_lineage_include_columns_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
    operation = lineage.operations[0]
    run = lineage.runs[0]
    job = lineage.jobs[0]
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
            "include_column_lineage": True,
        },
    )
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]) + operation_parents_to_json([operation]),
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
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "FILTER",
                                "JOIN",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": operations_to_json([operation]),
        },
    }


async def test_get_run_lineage_include_columns_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
    run = lineage.runs[0]
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.run_id == run.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.run_id == run.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    [run] = await enrich_runs([run], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": str(run.id),
            "include_column_lineage": True,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json([run]),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
                *inputs_to_json(merge_io_by_runs(inputs), granularity="RUN"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
                *outputs_to_json(merge_io_by_runs(outputs), granularity="RUN"),
            ],
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                    "TRANSFORMATION_MASKING",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "FILTER",
                                "JOIN",
                                "GROUP_BY",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json([run]),
            "operations": {},
        },
    }


async def test_get_job_lineage_include_columns_with_combined_transformations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage_with_column_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "include_column_lineage": True,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "IDENTITY",
                                    "TRANSFORMATION",
                                    "TRANSFORMATION_MASKING",
                                    "AGGREGATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "FILTER",
                                "JOIN",
                                "GROUP_BY",
                                "SORT",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_include_columns_with_depth_and_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
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
            "include_column_lineage": True,
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
            "direct_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "AGGREGATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "JOIN",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_dataset_lineage_include_columns_with_depth_and_granularity_job(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
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
            "include_column_lineage": True,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "AGGREGATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "JOIN",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_include_columns_with_depth_and_granularity_operation(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
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
            "include_column_lineage": True,
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
            "direct_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": {
                        "direct_target_column": [
                            {
                                "field": "direct_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "AGGREGATION",
                                ],
                            },
                        ],
                    },
                },
            ],
            "indirect_column_lineage": [
                {
                    "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                    "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                    "fields": [
                        {
                            "field": "indirect_source_column",
                            "last_used_at": format_datetime(lineage.operations[0].created_at),
                            "types": [
                                "JOIN",
                            ],
                        },
                    ],
                },
            ],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
    }


async def test_get_operation_lineage_include_columns_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
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
            "include_column_lineage": True,
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
            "direct_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[0].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[1].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "indirect_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[1].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": operations_to_json(operations),
        },
    }


async def test_get_run_lineage_include_columns_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage

    first_level_run = lineage.runs[0]

    # Go runs[first level] -> datasets[second level]
    first_level_inputs = [input for input in lineage.inputs if input.run_id == first_level_run.id]
    first_level_outputs = [output for output in lineage.outputs if output.run_id == first_level_run.id]
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    first_level_datasets = [dataset for dataset in lineage.datasets if dataset.id in first_level_dataset_ids]
    assert first_level_datasets

    # Go datasets[second level] -> runs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_run_ids = {input.run_id for input in second_level_inputs}
    second_level_output_run_ids = {output.run_id for output in second_level_outputs}
    second_level_run_ids = second_level_input_run_ids | second_level_output_run_ids - {first_level_run.id}
    assert second_level_run_ids

    # Go runs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.run_id in second_level_output_run_ids]
    third_level_outputs = [output for output in lineage.outputs if output.run_id in second_level_input_run_ids]
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

    run_ids = {first_level_run.id} | second_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]
    assert jobs

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/runs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": first_level_run.created_at.isoformat(),
            "start_node_id": str(first_level_run.id),
            "depth": 3,
            "include_column_lineage": True,
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
            "direct_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[0].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[1].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "indirect_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[1].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_job_lineage_include_columns_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
    # Select only relations marked with *
    # D1 -*> J1 -*> D2
    # D2 -*> J2 -*> D3
    # D3 --> J3 --> D4

    job = lineage.jobs[0]

    # Go job[first level] -> datasets[second level]
    first_level_inputs = [input for input in lineage.inputs if input.job_id == job.id]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == job.id]
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_job_ids = {input.job_id for input in second_level_inputs}
    second_level_output_job_ids = {output.job_id for output in second_level_outputs}
    second_level_job_ids = second_level_input_job_ids | second_level_output_job_ids - {job.id}
    second_level_jobs = [job for job in lineage.jobs if job.id in second_level_job_ids]
    assert second_level_jobs

    # Go jobs[second level] -> datasets[third level]
    # There are more levels in this graph, but we stop here
    third_level_inputs = [input for input in lineage.inputs if input.job_id in second_level_output_job_ids]
    third_level_outputs = [output for output in lineage.outputs if output.job_id in second_level_input_job_ids]
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

    jobs = [job] + second_level_jobs

    jobs = await enrich_jobs(jobs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "depth": 3,
            "include_column_lineage": True,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[0].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[1].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "indirect_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[1].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_and_column_lineage(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth_and_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth_and_with_column_lineage
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
            "include_column_lineage": True,
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
            "direct_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[0].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(lineage.operations[1].created_at),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "indirect_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[0].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(lineage.operations[1].created_at),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.outputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }


async def test_get_dataset_lineage_with_granularity_dataset_and_column_lineage_for_long_running_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_for_long_running_operations_with_column_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_for_long_running_operations_with_column_lineage
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
            "include_column_lineage": True,
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
            "direct_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(
                                        lineage.operations[0].created_at + timedelta(hours=2),
                                    ),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": {
                            "direct_target_column": [
                                {
                                    "field": "direct_source_column",
                                    "last_used_at": format_datetime(
                                        lineage.operations[1].created_at + timedelta(hours=2),
                                    ),
                                    "types": [
                                        "AGGREGATION",
                                    ],
                                },
                            ],
                        },
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
            "indirect_column_lineage": sorted(
                [
                    {
                        "from": {"id": str(lineage.datasets[0].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(
                                    lineage.operations[0].created_at + timedelta(hours=2),
                                ),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                    {
                        "from": {"id": str(lineage.datasets[1].id), "kind": "DATASET"},
                        "to": {"id": str(lineage.datasets[2].id), "kind": "DATASET"},
                        "fields": [
                            {
                                "field": "indirect_source_column",
                                "last_used_at": format_datetime(
                                    lineage.operations[1].created_at + timedelta(hours=2),
                                ),
                                "types": [
                                    "JOIN",
                                ],
                            },
                        ],
                    },
                ],
                key=lambda x: (x["from"]["id"], x["to"]["id"]),
            ),
        },
        "nodes": {
            "datasets": {
                str(dataset.id): {
                    "id": str(dataset.id),
                    "name": dataset.name,
                    "location": location_to_json(dataset.location),
                    "schema": schema_to_json(lineage.outputs[0].schema, "EXACT_MATCH"),
                }
                for dataset in datasets
            },
            "jobs": {},
            "runs": {},
            "operations": {},
        },
    }
