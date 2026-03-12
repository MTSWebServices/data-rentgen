from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Job, OutputType, Run
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.schema import create_schema
from tests.test_server.utils.convert_to_json import (
    datasets_to_json,
    inputs_to_json,
    jobs_ancestors_to_json,
    jobs_to_json,
    outputs_to_json,
    run_parents_to_json,
    runs_ancestors_to_json,
    runs_to_json,
    symlinks_to_json,
)
from tests.test_server.utils.enrich import enrich_datasets, enrich_jobs, enrich_runs
from tests.test_server.utils.lineage_result import LineageResult
from tests.test_server.utils.merge import merge_io_by_jobs, merge_io_by_runs

pytestmark = [pytest.mark.server, pytest.mark.asyncio, pytest.mark.lineage]


async def test_get_job_lineage_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.get("v1/jobs/lineage")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_job_lineage_via_personal_token_is_allowed(
    test_client: AsyncClient,
    job: Job,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()


async def test_get_job_lineage_no_runs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": datetime.now(tz=timezone.utc).isoformat(),
            "start_node_id": job.id,
        },
    )

    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {},
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_no_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    # runs without operations are excluded
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {},
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_no_inputs_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    job: Job,
    run: Run,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
        },
    )

    [run] = await enrich_runs([run], async_session)
    [job] = await enrich_jobs([job], async_session)

    assert response.status_code == HTTPStatus.OK, response.json()
    # runs without inputs/outputs are excluded,
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": [],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": {},
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_simple(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
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
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_direction_downstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    job = lineage.jobs[0]

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "DOWNSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": [],
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs=outputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_direction_upstream(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    job = lineage.jobs[0]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    dataset_ids = {input.dataset_id for input in inputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "direction": "UPSTREAM",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, inputs=inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    three_days_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = three_days_lineage
    job = lineage.jobs[0]
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)
    until = since + timedelta(seconds=1)

    inputs = [input for input in lineage.inputs if input.job_id == job.id and since <= input.created_at <= until]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id and since <= output.created_at <= until]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "until": until.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    simple_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = simple_lineage
    job = lineage.jobs[0]
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    inputs = [input for input in lineage.inputs if input.job_id == job.id and since <= input.created_at]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id and since <= output.created_at]
    assert outputs

    # Only runs with relations are returned
    run_ids = {input.run_id for input in inputs} | {output.run_id for output in outputs}
    runs = [run for run in lineage.runs if run.id in run_ids]
    assert runs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "granularity": "RUN",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "ancestors": [],
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
            "jobs": jobs_to_json([job]),
            "runs": runs_to_json(runs),
            "operations": {},
        },
    }


async def test_get_job_lineage_with_depth(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
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
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
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


async def test_get_job_lineage_with_depth_and_granularity_run(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_depth: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_depth
    # Select only relations marked with *
    # J1 -*> R1, D1 -*> R1 -*> D2
    # J2 -*> R2, D2 -*> R2 -*> D3
    # J3 --> R3, D3 --> R3 --> D4

    first_level_job_id = lineage.jobs[0].id

    # Go job[first level] -> runs[first level] -> datasets[first level]
    first_level_inputs = [input for input in lineage.inputs if input.job_id == first_level_job_id]
    first_level_outputs = [output for output in lineage.outputs if output.job_id == first_level_job_id]
    first_level_input_run_ids = {input.run_id for input in first_level_inputs}
    first_level_output_run_ids = {output.run_id for output in first_level_outputs}
    first_level_run_ids = first_level_input_run_ids | first_level_output_run_ids
    first_level_input_dataset_ids = {input.dataset_id for input in first_level_inputs}
    first_level_output_dataset_ids = {output.dataset_id for output in first_level_outputs}
    first_level_dataset_ids = first_level_input_dataset_ids | first_level_output_dataset_ids
    assert first_level_dataset_ids

    # Go datasets[first level] -> runs[second level] -> jobs[second level]
    second_level_inputs = [input for input in lineage.inputs if input.dataset_id in first_level_output_dataset_ids]
    second_level_outputs = [output for output in lineage.outputs if output.dataset_id in first_level_input_dataset_ids]
    second_level_input_run_ids = {input.run_id for input in second_level_inputs}
    second_level_output_run_ids = {output.run_id for output in second_level_outputs}
    second_level_run_ids = second_level_input_run_ids | second_level_output_run_ids - first_level_run_ids
    assert second_level_run_ids

    # Go runs[second level] -> datasets[third level]
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

    run_ids = first_level_run_ids | second_level_run_ids
    runs = [run for run in lineage.runs if run.id in run_ids]

    job_ids = {run.job_id for run in runs}
    jobs = [job for job in lineage.jobs if job.id in job_ids]

    jobs = await enrich_jobs(jobs, async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": first_level_job_id,
            "granularity": "RUN",
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "ancestors": [],
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


async def test_get_job_lineage_with_depth_ignore_cycles(
    test_client: AsyncClient,
    async_session: AsyncSession,
    cyclic_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = cyclic_lineage
    # Select all relations:
    # J1 -*> R1 -*> O1, D1 -*> O1 -*> D2
    # J2 -*> R2 -*> O2, D2 -*> O2 -*> D1

    # We can start at any job
    job = lineage.jobs[0]

    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)
    since = min(run.created_at for run in lineage.runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(lineage.outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, lineage.outputs, lineage.inputs),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_depth_ignore_unrelated_datasets(
    test_client: AsyncClient,
    async_session: AsyncSession,
    branchy_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = branchy_lineage
    # Start from J1, build lineage with direction=BOTH
    job = lineage.jobs[1]

    # Select only relations marked with *
    # D0   D1
    #  *\ /*
    #    J0 -> D2
    #     *\
    #      D3  D4
    #       *\ /*
    #         J1 -*> D5
    #         *\
    #          D6  D7
    #           *\ /
    #             J2 -*> D8
    #              *\
    #                D9

    datasets = [
        lineage.datasets[0],
        lineage.datasets[1],
        # D2 is not a part of (J1,D3,J0,D0,D1) input chain
        lineage.datasets[3],
        lineage.datasets[4],
        lineage.datasets[5],
        lineage.datasets[6],
        # D7 is not a part of (J1,D6,J2,D8,D9) output chain
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
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "depth": 3,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
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


async def test_get_job_lineage_with_symlinks(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_symlinks

    job = lineage.jobs[1]
    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    assert dataset_ids

    # Dataset from symlinks appear only as SYMLINK relation, but not as INPUT, because of depth=1
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

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": symlinks_to_json(dataset_symlinks),
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_symlink_without_input_output(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_unconnected_symlinks: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_unconnected_symlinks

    job = lineage.jobs[0]
    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
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

    [job] = await enrich_jobs([job], async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in lineage.runs if run.job_id == job.id)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": symlinks_to_json(dataset_symlinks),
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_unmergeable_inputs_and_outputs(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # make every input and output a different schema -> grouping by Job returns None.
    # make every output a different type -> grouping by Job returns None.
    for input in lineage.inputs:
        schema = await create_schema(async_session)
        input.schema_id = schema.id
        input.schema = schema
        await async_session.merge(input)

    output_types = list(OutputType)
    for i, output in enumerate(lineage.outputs):
        schema = await create_schema(async_session)
        output.schema_id = schema.id
        output.schema = schema
        output.type = output_types[i % len(output_types)]
        await async_session.merge(output)

    await async_session.commit()

    job = lineage.jobs[0]
    runs = [run for run in lineage.runs if run.job_id == job.id]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            "outputs": outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_empty_io_stats_and_schema(
    test_client: AsyncClient,
    async_session: AsyncSession,
    duplicated_lineage: LineageResult,
    mocked_user: MockedUser,
):
    lineage = duplicated_lineage

    # clear input/output stats and schema.
    for input in lineage.inputs:
        input.schema_id = None
        input.schema = None
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None
        await async_session.merge(input)

    for output in lineage.outputs:
        output.schema_id = None
        output.schema = None
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None
        await async_session.merge(output)

    await async_session.commit()

    job = lineage.jobs[0]
    runs = [run for run in lineage.runs if run.job_id == job.id]

    inputs = [input for input in lineage.inputs if input.job_id == job.id]
    assert inputs

    outputs = [output for output in lineage.outputs if output.job_id == job.id]
    assert outputs

    dataset_ids = {input.dataset_id for input in inputs} | {output.dataset_id for output in outputs}
    datasets = [dataset for dataset in lineage.datasets if dataset.id in dataset_ids]
    assert datasets

    [job] = await enrich_jobs([job], async_session)
    runs = await enrich_runs(runs, async_session)
    datasets = await enrich_datasets(datasets, async_session)
    since = min(run.created_at for run in runs)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    # merge_io_by_jobs sums empty bytes, rows and files, producing 0 instead of None.
    # override that
    merged_inputs = merge_io_by_jobs(inputs)
    for input in merged_inputs:
        input.num_bytes = None
        input.num_rows = None
        input.num_files = None

    merged_outputs = merge_io_by_jobs(outputs)
    for output in merged_outputs:
        output.num_bytes = None
        output.num_rows = None
        output.num_files = None

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": inputs_to_json(merged_inputs, granularity="JOB"),
            "outputs": outputs_to_json(merged_outputs, granularity="JOB"),
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_for_long_running_operations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_for_long_running_operations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_for_long_running_operations

    job = lineage.jobs[0]

    # use only latest IO for each operation+dataset
    raw_inputs = [input for input in lineage.inputs if input.job_id == job.id]
    latest_inputs = {}
    for input in raw_inputs:
        index = (input.operation_id, input.dataset_id)
        existing = latest_inputs.get(index)
        if not existing or input.created_at > existing.created_at:
            latest_inputs[index] = input
    inputs = list(latest_inputs.values())
    assert inputs

    raw_outputs = [output for output in lineage.outputs if output.job_id == job.id]
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

    run = next(run for run in lineage.runs if run.job_id == job.id)

    datasets = await enrich_datasets(datasets, async_session)
    [job] = await enrich_jobs([job], async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": run.created_at.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": [],
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(inputs), granularity="JOB"),
            ],
            "outputs": [
                *outputs_to_json(merge_io_by_jobs(outputs), granularity="JOB"),
            ],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets, outputs, inputs),
            "jobs": jobs_to_json([job]),
            "runs": {},
            "operations": {},
        },
    }


async def test_get_job_lineage_with_ancestor_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_parent_run_relations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_parent_run_relations
    run = lineage.runs[-1]
    job = next(job for job in lineage.jobs if run.job_id == job.id)
    since = run.created_at

    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": jobs_ancestors_to_json(jobs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
            ],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "runs": {},
            "jobs": jobs_to_json(jobs),
            "operations": {},
        },
    }


async def test_get_job_lineage_with_granularity_run_and_ancestor_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_parent_run_relations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_parent_run_relations
    run = lineage.runs[-1]
    job = next(job for job in lineage.jobs if run.job_id == job.id)
    since = run.created_at

    lineage.runs.pop(-2)
    runs = await enrich_runs(lineage.runs, async_session)
    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
            "granularity": "RUN",
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": run_parents_to_json(runs),
            "ancestors": jobs_ancestors_to_json(jobs) + runs_ancestors_to_json(runs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
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
            "operations": {},
        },
    }


async def test_get_job_lineage_with_descendant_relations(
    test_client: AsyncClient,
    async_session: AsyncSession,
    lineage_with_parent_run_relations: LineageResult,
    mocked_user: MockedUser,
):
    lineage = lineage_with_parent_run_relations
    run = lineage.runs[0]
    job = next(job for job in lineage.jobs if run.job_id == job.id)
    since = run.created_at

    jobs = await enrich_jobs(lineage.jobs, async_session)
    datasets = await enrich_datasets(lineage.datasets, async_session)

    response = await test_client.get(
        "v1/jobs/lineage",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={
            "since": since.isoformat(),
            "start_node_id": job.id,
        },
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "relations": {
            "parents": [],
            "ancestors": jobs_ancestors_to_json(jobs),
            "symlinks": [],
            "inputs": [
                *inputs_to_json(merge_io_by_jobs(lineage.inputs), granularity="JOB"),
            ],
            "outputs": [],
            "direct_column_lineage": [],
            "indirect_column_lineage": [],
        },
        "nodes": {
            "datasets": datasets_to_json(datasets),
            "jobs": jobs_to_json(jobs),
            "runs": {},
            "operations": {},
        },
    }
