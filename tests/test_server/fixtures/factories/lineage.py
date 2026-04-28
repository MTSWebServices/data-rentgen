from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import DatasetColumnRelationType, DatasetSymlinkType, RunStatus, User
from data_rentgen.db.models.output import OutputType
from data_rentgen.utils.uuid import generate_static_uuid
from tests.test_server.fixtures.factories.lineage_builder import LineageBuilder
from tests.test_server.fixtures.factories.relations import create_column_lineage, create_column_relation
from tests.test_server.utils.delete import clean_db
from tests.test_server.utils.lineage_result import LineageResult


@pytest_asyncio.fixture()
async def simple_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Two independent operations, run twice:
    J1 -> R1 -> O1, D1 -> O1 -> D2
    J1 -> R1 -> O2, D3 -> O2 -> D4
    J1 -> R2 -> O3, D1 -> O3 -> D2
    J1 -> R2 -> O4, D3 -> O4 -> D4
    """

    num_runs = 2
    num_operations = 2
    num_datasets = 4

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        job_type = await builder.create_job_type(key="simple_lineage_job_type")
        job = await builder.create_job(
            key="simple_lineage_job",
            location_key="simple_lineage_job_location",
            job_type=job_type,
        )
        created_at = datetime.now(tz=UTC)

        for n in range(num_runs):
            run = await builder.create_run(
                key=f"run_{n}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "created_at": created_at + timedelta(seconds=0.1 * n),
                    "started_by_user_id": user.id,
                },
            )

            # Each run has 2 operations
            operations = [
                await builder.create_operation(
                    key=f"run_{n}_operation_{i}",
                    run=run,
                    operation_kwargs={
                        "run_id": run.id,
                        "created_at": run.created_at + timedelta(seconds=0.2 * i),
                    },
                )
                for i in range(num_operations)
            ]

            dataset_locations = [
                await builder.create_location(key=f"run_{n}_dataset_location_{i}") for i in range(num_datasets)
            ]
            datasets = [
                await builder.create_dataset(
                    key=f"run_{n}_dataset_{i}",
                    location=location,
                )
                for i, location in enumerate(dataset_locations)
            ]

            schema = await builder.create_schema(key=f"run_{n}_schema")

            [
                await builder.create_input(
                    key=f"run_{n}_input_{i}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[2 * i],
                    schema=schema,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[2 * i].id,
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]

            [
                await builder.create_output(
                    key=f"run_{n}_output_{i}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[2 * i + 1],
                    output_type=OutputType.APPEND,
                    schema=schema,
                    output_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[2 * i + 1].id,
                        "type": OutputType.APPEND,
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def three_days_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Several of J -> R -> O, connected via same pair of datasets:
    J0 -> R0 -> O0, D0 -> O0 -> D1
    J0 -> R0 -> O1, D1 -> O1 -> D2
    J1 -> R1 -> O2, D0 -> O2 -> D1
    J1 -> R1 -> O3, D1 -> O3 -> D2
    J2 -> R2 -> O4, D0 -> O4 -> D1
    J2 -> R2 -> O5, D1 -> O5 -> D2
    Runs are 1 day apart.
    """

    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        job_type = await builder.create_job_type(key="three_days_lineage_job_type")
        job = await builder.create_job(
            key="three_days_lineage_job",
            location_key="three_days_lineage_job_location",
            job_type=job_type,
        )

        for day in range(3):
            run = await builder.create_run(
                key=f"three_days_run_{day}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "created_at": created_at + timedelta(days=day),
                    "started_by_user_id": user.id,
                },
            )

            operations = [
                await builder.create_operation(
                    key=f"three_days_run_{day}_operation_{i}",
                    run=run,
                    operation_kwargs={
                        "run_id": run.id,
                        "created_at": run.created_at + timedelta(seconds=0.2 * i),
                    },
                )
                for i in range(2)
            ]

            dataset_locations = [
                await builder.create_location(key=f"three_days_run_{day}_dataset_location_{i}") for i in range(3)
            ]
            datasets = [
                await builder.create_dataset(
                    key=f"three_days_run_{day}_dataset_{i}",
                    location=location,
                )
                for i, location in enumerate(dataset_locations)
            ]

            schema = await builder.create_schema(key=f"three_days_run_{day}_schema")

            [
                await builder.create_input(
                    key=f"three_days_run_{day}_input_{i}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[i],
                    schema=schema,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[i].id,
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]

            [
                await builder.create_output(
                    key=f"three_days_run_{day}_output_{i}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[i + 1],
                    output_type=OutputType.APPEND,
                    schema=schema,
                    output_kwargs={
                        "created_at": operation.created_at + timedelta(seconds=0.1),
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[i + 1].id,
                        "type": OutputType.APPEND,
                        "schema_id": schema.id,
                    },
                )
                for i, operation in enumerate(operations)
            ]

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_depth(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Three trees of J -> R -> O, connected via datasets:
    J1 -> R1 -> O1, D1 -> O1 -> D2
    J2 -> R2 -> O2, D2 -> O2 -> D3
    J3 -> R3 -> O3, D3 -> O3 -> D4
    J4 -> R4 -> O4, D4 -> O4 -> D5
    """

    num_datasets = 5
    num_jobs = 4
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(key=f"lineage_with_depth_dataset_location_{i}") for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"lineage_with_depth_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        schema = await builder.create_schema(key="lineage_with_depth_schema")

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
            job_type = await builder.create_job_type(key=f"lineage_with_depth_job_type_{i}")
            job = await builder.create_job(
                key=f"lineage_with_depth_job_{i}",
                location_key=f"lineage_with_depth_job_location_{i}",
                job_type=job_type,
            )
            run = await builder.create_run(
                key=f"lineage_with_depth_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )
            operation = await builder.create_operation(
                key=f"lineage_with_depth_operation_{i}",
                run=run,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                },
            )
            await builder.create_input(
                key=f"lineage_with_depth_input_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[i],
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[i].id,
                    "schema_id": schema.id,
                },
            )

            await builder.create_output(
                key=f"lineage_with_depth_output_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[i + 1],
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[i + 1].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def cyclic_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Two trees of J -> R -> O, forming a cycle:
    J1 -> R1 -> O1, D1 -> O1 -> D2
    J2 -> R2 -> O2, D2 -> O2 -> D1
    """

    num_datasets = 2
    num_jobs = 2
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(key=f"cyclic_lineage_dataset_location_{i}") for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"cyclic_lineage_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        schema = await builder.create_schema(key="cyclic_lineage_schema")

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
            from_dataset, to_dataset = (datasets[0], datasets[1]) if i == 0 else (datasets[1], datasets[0])

            job_type = await builder.create_job_type(key=f"cyclic_lineage_job_type_{i}")
            job = await builder.create_job(
                key=f"cyclic_lineage_job_{i}",
                location_key=f"cyclic_lineage_job_location_{i}",
                job_type=job_type,
            )
            run = await builder.create_run(
                key=f"cyclic_lineage_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )
            operation = await builder.create_operation(
                key=f"cyclic_lineage_operation_{i}",
                run=run,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                },
            )
            await builder.create_input(
                key=f"cyclic_lineage_input_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=from_dataset,
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": from_dataset.id,
                    "schema_id": schema.id,
                },
            )

            await builder.create_output(
                key=f"cyclic_lineage_output_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=to_dataset,
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": to_dataset.id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def self_referencing_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Example then table can be its own source:
    J1 -> R1 -> O1, D1 -> O1 -> D1  # reading duplicates and removing them
    """

    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_location = await builder.create_location(key="self_ref_dataset_location")
        dataset = await builder.create_dataset(key="self_ref_dataset", location=dataset_location)

        schema = await builder.create_schema(key="self_ref_schema")

        job_type = await builder.create_job_type(key="self_ref_job_type")
        job = await builder.create_job(
            key="self_ref_job",
            location_key="self_ref_job_location",
            job_type=job_type,
        )
        run = await builder.create_run(
            key="self_ref_run",
            job=job,
            run_kwargs={
                "job_id": job.id,
                "started_by_user_id": user.id,
                "created_at": created_at + timedelta(seconds=5),
            },
        )
        operation = await builder.create_operation(
            key="self_ref_operation",
            run=run,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=1),
            },
        )
        await builder.create_input(
            key="self_ref_input",
            operation=operation,
            run=run,
            job=job,
            dataset=dataset,
            schema=schema,
            input_kwargs={
                "created_at": operation.created_at,
                "operation_id": operation.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": dataset.id,
                "schema_id": schema.id,
            },
        )

        await builder.create_output(
            key="self_ref_output",
            operation=operation,
            run=run,
            job=job,
            dataset=dataset,
            output_type=OutputType.OVERWRITE,
            schema=schema,
            output_kwargs={
                "created_at": operation.created_at,
                "operation_id": operation.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": dataset.id,
                "type": OutputType.OVERWRITE,
                "schema_id": schema.id,
            },
        )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_non_connected_operations(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Run interacted with 2 datasets, but in different operations:
    J1 -> R1 -> O1, D1 -> O1        # SELECT max(id) FROM table1
    J1 -> R1 -> O2,       O2 -> D2  # INSERT INTO table1 VALUES
    """

    num_datasets = 2
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(key=f"non_connected_dataset_location_{i}") for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"non_connected_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        schema = await builder.create_schema(key="non_connected_schema")

        job_type = await builder.create_job_type(key="non_connected_job_type")
        job = await builder.create_job(
            key="non_connected_job",
            location_key="non_connected_job_location",
            job_type=job_type,
        )
        run = await builder.create_run(
            key="non_connected_run",
            job=job,
            run_kwargs={
                "job_id": job.id,
                "started_by_user_id": user.id,
                "created_at": created_at + timedelta(seconds=10),
            },
        )
        operation1 = await builder.create_operation(
            key="non_connected_operation_1",
            run=run,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=1),
            },
        )
        await builder.create_input(
            key="non_connected_input_1",
            operation=operation1,
            run=run,
            job=job,
            dataset=datasets[0],
            schema=schema,
            input_kwargs={
                "created_at": operation1.created_at,
                "operation_id": operation1.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": datasets[0].id,
                "schema_id": schema.id,
            },
        )

        operation2 = await builder.create_operation(
            key="non_connected_operation_2",
            run=run,
            operation_kwargs={
                "run_id": run.id,
                "created_at": run.created_at + timedelta(seconds=2),
            },
        )
        await builder.create_output(
            key="non_connected_output_2",
            operation=operation2,
            run=run,
            job=job,
            dataset=datasets[1],
            output_type=OutputType.APPEND,
            schema=schema,
            output_kwargs={
                "created_at": operation2.created_at,
                "operation_id": operation2.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": datasets[1].id,
                "type": OutputType.APPEND,
                "schema_id": schema.id,
            },
        )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def duplicated_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Two trees of J -> R -> O, interacting with the same dataset multiple times:
    J0 -> R0 -> O0, D0 -> O0 -> D1
    J0 -> R0 -> O1, D0 -> O1 -> D1
    J0 -> R1 -> O2, D0 -> O2 -> D1
    J0 -> R1 -> O3, D0 -> O3 -> D1
    J1 -> R2 -> O4, D0 -> O4 -> D1
    J1 -> R2 -> O5, D0 -> O5 -> D1
    J1 -> R3 -> O6, D0 -> O6 -> D1
    J1 -> R3 -> O7, D0 -> O7 -> D1
    """

    num_datasets = 2
    num_jobs = 2
    runs_per_job = 2
    operations_per_run = 2
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(key=f"duplicated_lineage_dataset_location_{i}") for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"duplicated_lineage_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        schema = await builder.create_schema(key="duplicated_lineage_schema")

        # Create a job, run and operation with IO datasets.
        for i in range(num_jobs):
            job_type = await builder.create_job_type(key=f"duplicated_lineage_job_type_{i}")
            job = await builder.create_job(
                key=f"duplicated_lineage_job_{i}",
                location_key=f"duplicated_lineage_job_location_{i}",
                job_type=job_type,
            )
            runs = [
                await builder.create_run(
                    key=f"duplicated_lineage_job_{i}_run_{run_idx}",
                    job=job,
                    run_kwargs={
                        "job_id": job.id,
                        "started_by_user_id": user.id,
                        "created_at": created_at + timedelta(seconds=i),
                    },
                )
                for run_idx in range(runs_per_job)
            ]
            operations = [
                await builder.create_operation(
                    key=f"duplicated_lineage_job_{i}_run_{run_idx}_operation_{operation_idx}",
                    run=run,
                    operation_kwargs={
                        "run_id": run.id,
                        "created_at": run.created_at + timedelta(seconds=0.2),
                    },
                )
                for run_idx, run in enumerate(runs)
                for operation_idx in range(operations_per_run)
            ]
            [
                await builder.create_input(
                    key=f"duplicated_lineage_job_{i}_input_{op_idx}",
                    operation=operation,
                    run=next(run for run in runs if run.id == operation.run_id),
                    job=job,
                    dataset=datasets[0],
                    schema=schema,
                    input_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[0].id,
                        "schema_id": schema.id,
                    },
                )
                for op_idx, operation in enumerate(operations)
            ]

            [
                await builder.create_output(
                    key=f"duplicated_lineage_job_{i}_output_{op_idx}",
                    operation=operation,
                    run=next(run for run in runs if run.id == operation.run_id),
                    job=job,
                    dataset=datasets[1],
                    output_type=OutputType.APPEND,
                    schema=schema,
                    output_kwargs={
                        "created_at": operation.created_at,
                        "operation_id": operation.id,
                        "run_id": operation.run_id,
                        "job_id": job.id,
                        "dataset_id": datasets[1].id,
                        "type": OutputType.APPEND,
                        "schema_id": schema.id,
                    },
                )
                for op_idx, operation in enumerate(operations)
            ]

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def branchy_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
):
    """
    Three trees of J -> R -> O, connected via D3 and D6, but having other inputs & outputs:
             D0   D1
               \\ /
    J0 -> R0 -> O0 -> D2
                 \
                  D3  D4
                   \\ /
        J1 -> R1 -> O1 -> D5
                     \
                      D6  D7
                       \\ /
            J2 -> R2 -> O2 -> D8
                         \
                          D9
    """

    num_datasets = 10
    num_jobs = 3
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(key=f"branchy_dataset_location_{i}") for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"branchy_dataset_{i}",
                location=location,
                dataset_kwargs={"name": f"dataset_{i}"},
            )
            for i, location in enumerate(dataset_locations)
        ]
        job_type = await builder.create_job_type(key="branchy_job_type")
        jobs = [
            await builder.create_job(
                key=f"branchy_job_{i}",
                location_key=f"branchy_job_location_{i}",
                job_type=job_type,
                job_kwargs={"name": f"job_{i}"},
            )
            for i in range(num_jobs)
        ]
        runs = [
            await builder.create_run(
                key=f"branchy_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                    "external_id": f"run_{i}",
                },
            )
            for i, job in enumerate(jobs)
        ]
        operations = [
            await builder.create_operation(
                key=f"branchy_operation_{i}",
                run=run,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "name": f"operation_{i}",
                },
            )
            for i, run in enumerate(runs)
        ]
        schema = await builder.create_schema(key="branchy_schema")

        [
            await builder.create_input(
                key=f"branchy_input_{i}_0",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[3 * i],
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i].id,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs, strict=False))
        ] + [
            await builder.create_input(
                key=f"branchy_input_{i}_1",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[3 * i + 1],
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i + 1].id,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs, strict=False))
        ]

        [
            await builder.create_output(
                key=f"branchy_output_{i}_0",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[3 * i + 2],
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i + 2].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs, strict=False))
        ] + [
            await builder.create_output(
                key=f"branchy_output_{i}_1",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[3 * i + 3],
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": run.id,
                    "job_id": job.id,
                    "dataset_id": datasets[3 * i + 3].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, run, job) in enumerate(zip(operations, runs, jobs, strict=False))
        ]

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_symlinks(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Three trees of J -> R -> O, connected to datasets via symlinks:
    J1 -> R1 -> O1, D1 -> O1 -> D2S
    J2 -> R2 -> O2, D2 -> O2 -> D3S
    J3 -> R3 -> O3, D3 -> O2 -> D4S

    TODO: This fixture creates a different structure. (D1 -> O1 -> D1S). It must be fixed.
    """

    created_at = datetime.now(tz=UTC)
    num_datasets = 4
    num_jobs = 3

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(
                key=f"lineage_with_symlinks_dataset_location_{i}",
                location_kwargs={"type": "hdfs"},
            )
            for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"lineage_with_symlinks_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        symlink_locations = [
            await builder.create_location(
                key=f"lineage_with_symlinks_symlink_location_{i}",
                location_kwargs={"type": "hive"},
            )
            for i in range(num_datasets)
        ]
        symlink_datasets = [
            await builder.create_dataset(
                key=f"lineage_with_symlinks_symlink_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(symlink_locations)
        ]
        # Make symlinks
        for i, (dataset, symlink_dataset) in enumerate(zip(datasets, symlink_datasets, strict=False)):
            await builder.create_dataset_symlink(
                key=f"lineage_with_symlinks_metastore_{i}",
                from_dataset=dataset,
                to_dataset=symlink_dataset,
                type=DatasetSymlinkType.METASTORE,
            )

            await builder.create_dataset_symlink(
                key=f"lineage_with_symlinks_warehouse_{i}",
                from_dataset=symlink_dataset,
                to_dataset=dataset,
                type=DatasetSymlinkType.WAREHOUSE,
            )

        schema = await builder.create_schema(key="lineage_with_symlinks_schema")

        # Make graphs
        for i in range(num_jobs):
            job_type = await builder.create_job_type(key=f"lineage_with_symlinks_job_type_{i}")
            job = await builder.create_job(
                key=f"lineage_with_symlinks_job_{i}",
                location_key=f"lineage_with_symlinks_job_location_{i}",
                job_type=job_type,
            )
            run = await builder.create_run(
                key=f"lineage_with_symlinks_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )
            operation = await builder.create_operation(
                key=f"lineage_with_symlinks_operation_{i}",
                run=run,
                operation_kwargs={
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "run_id": run.id,
                },
            )
            await builder.create_input(
                key=f"lineage_with_symlinks_input_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[i],
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": datasets[i].id,
                    "schema_id": schema.id,
                },
            )
            await builder.create_output(
                key=f"lineage_with_symlinks_output_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=symlink_datasets[i],
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": symlink_datasets[i].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_symlinks_dataset_granularity(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Three trees of J -> R -> O, connected to datasets via symlinks:
    J1 -> R1 -> O1, D1 -> O1 -> D2S
    J2 -> R2 -> O2, D2 -> O2 -> D3S
    J3 -> R3 -> O3, D3 -> O2 -> D4S
    """

    created_at = datetime.now(tz=UTC)
    num_datasets = 4
    num_jobs = 3

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(
                key=f"lineage_with_symlinks_granularity_dataset_location_{i}",
                location_kwargs={"type": "hdfs"},
            )
            for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"lineage_with_symlinks_granularity_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]
        symlink_locations = [
            await builder.create_location(
                key=f"lineage_with_symlinks_granularity_symlink_location_{i}",
                location_kwargs={"type": "hive"},
            )
            for i in range(num_datasets)
        ]
        symlink_datasets = [
            await builder.create_dataset(
                key=f"lineage_with_symlinks_granularity_symlink_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(symlink_locations)
        ]
        # Make symlinks
        for i, (dataset, symlink_dataset) in enumerate(zip(datasets, symlink_datasets, strict=False)):
            await builder.create_dataset_symlink(
                key=f"lineage_with_symlinks_granularity_metastore_{i}",
                from_dataset=dataset,
                to_dataset=symlink_dataset,
                type=DatasetSymlinkType.METASTORE,
            )

            await builder.create_dataset_symlink(
                key=f"lineage_with_symlinks_granularity_warehouse_{i}",
                from_dataset=symlink_dataset,
                to_dataset=dataset,
                type=DatasetSymlinkType.WAREHOUSE,
            )

        schema = await builder.create_schema(key="lineage_with_symlinks_granularity_schema")

        # Make graphs
        for i in range(num_jobs):
            job_type = await builder.create_job_type(key=f"lineage_with_symlinks_granularity_job_type_{i}")
            job = await builder.create_job(
                key=f"lineage_with_symlinks_granularity_job_{i}",
                location_key=f"lineage_with_symlinks_granularity_job_location_{i}",
                job_type=job_type,
            )
            run = await builder.create_run(
                key=f"lineage_with_symlinks_granularity_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )
            operation = await builder.create_operation(
                key=f"lineage_with_symlinks_granularity_operation_{i}",
                run=run,
                operation_kwargs={
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "run_id": run.id,
                },
            )
            await builder.create_input(
                key=f"lineage_with_symlinks_granularity_input_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=datasets[i],
                schema=schema,
                input_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": datasets[i].id,
                    "schema_id": schema.id,
                },
            )
            await builder.create_output(
                key=f"lineage_with_symlinks_granularity_output_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=symlink_datasets[i + 1],
                output_type=OutputType.APPEND,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": symlink_datasets[i + 1].id,
                    "type": OutputType.APPEND,
                    "schema_id": schema.id,
                },
            )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_unconnected_symlinks(
    lineage_with_depth: LineageResult,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[LineageResult, None]:
    """
    Same as lineage_with_depth, but each dataset has also a symlink,
    not connected to any input or output.
    """

    lineage = lineage_with_depth

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        existing_datasets = lineage.datasets.copy()
        for i, dataset in enumerate(existing_datasets):
            another_location = await builder.create_location(key=f"unconnected_symlink_location_{i}")
            another_dataset = await builder.create_dataset(
                key=f"unconnected_symlink_dataset_{i}",
                location=another_location,
            )

            await builder.create_dataset_symlink(
                key=f"unconnected_symlink_metastore_{i}",
                from_dataset=another_dataset,
                to_dataset=dataset,
                type=DatasetSymlinkType.METASTORE,
            )

            await builder.create_dataset_symlink(
                key=f"unconnected_symlink_warehouse_{i}",
                from_dataset=dataset,
                to_dataset=another_dataset,
                type=DatasetSymlinkType.WAREHOUSE,
            )

        lineage_build = builder.build()
        lineage.datasets.extend(lineage_build.datasets)
        lineage.dataset_symlinks.extend(lineage_build.dataset_symlinks)

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def duplicated_lineage_with_column_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    duplicated_lineage: LineageResult,
) -> AsyncGenerator[LineageResult, None]:
    """
    Add column lineage to check relation type aggregation on different levels.
    O0 has direct and indirect relations for the same source-target columns.
    O1 has the same source-target columns as O0 but different relation types.
    O2 has the same source-target columns as O0/O1 but different relation types.

    Two trees of J -> R -> O, interacting with the same dataset multiple times:
    J0 -> R0 -> O0, D0 -> O0 -> D1
    J0 -> R0 -> O1, D0 -> O1 -> D1
    J0 -> R1 -> O2, D0 -> O2 -> D1
    J0 -> R1 -> O3, D0 -> O3 -> D1
    J1 -> R2 -> O4, D0 -> O4 -> D1
    J1 -> R2 -> O5, D0 -> O5 -> D1
    J1 -> R3 -> O6, D0 -> O6 -> D1
    J1 -> R3 -> O7, D0 -> O7 -> D1
    """
    operation_relations_matrix = (
        (0, 0, DatasetColumnRelationType.IDENTITY, DatasetColumnRelationType.FILTER),
        (0, 0, DatasetColumnRelationType.TRANSFORMATION, DatasetColumnRelationType.JOIN),
        (1, 0, DatasetColumnRelationType.TRANSFORMATION_MASKING, DatasetColumnRelationType.GROUP_BY),
        (2, 1, DatasetColumnRelationType.AGGREGATION, DatasetColumnRelationType.SORT),
    )

    lineage = duplicated_lineage
    async with async_session_maker() as async_session:
        for operation, run, direct_type, indirect_type in operation_relations_matrix:
            fingerprint = generate_static_uuid(direct_type.name + indirect_type.name)
            # Direct
            await create_column_relation(
                async_session,
                fingerprint=fingerprint,
                column_relation_kwargs={
                    "type": direct_type.value,
                    "source_column": "direct_source_column",
                    "target_column": "direct_target_column",
                },
            )
            # Indirect
            await create_column_relation(
                async_session,
                fingerprint=fingerprint,
                column_relation_kwargs={
                    "type": indirect_type.value,
                    "source_column": "indirect_source_column",
                    "target_column": None,
                },
            )
            await create_column_lineage(
                async_session,
                column_lineage_kwargs={
                    "created_at": lineage.operations[operation].created_at,
                    "operation": lineage.operations[operation],
                    "run": lineage.runs[run],
                    "job": lineage.jobs[0],
                    "source_dataset": lineage.datasets[0],
                    "target_dataset": lineage.datasets[1],
                    "fingerprint": fingerprint,
                },
            )

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def lineage_with_depth_and_with_column_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    lineage_with_depth: LineageResult,
) -> AsyncGenerator[LineageResult, None]:
    """
    Three trees of J -> R -> O, connected via datasets:
    J1 -> R1 -> O1, D1 -> O1 -> D2
    J2 -> R2 -> O2, D2 -> O2 -> D3
    J3 -> R3 -> O3, D3 -> O3 -> D4

    Each operation has the same column lineage, so we can test both depth and
    repeated lineage across different operations, runs, and jobs.
    """

    lineage = lineage_with_depth
    async with async_session_maker() as async_session:
        for i in range(len(lineage.jobs)):
            # Direct
            await create_column_relation(
                async_session,
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.AGGREGATION.value,
                    "source_column": "direct_source_column",
                    "target_column": "direct_target_column",
                },
            )
            # Indirect
            await create_column_relation(
                async_session,
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.JOIN.value,
                    "source_column": "indirect_source_column",
                    "target_column": None,
                },
            )

            await create_column_lineage(
                async_session,
                column_lineage_kwargs={
                    "created_at": lineage.operations[i].created_at,
                    "operation": lineage.operations[i],
                    "run": lineage.runs[i],
                    "job": lineage.jobs[i],
                    "source_dataset": lineage.datasets[i],
                    "target_dataset": lineage.datasets[i + 1],
                    "fingerprint": generate_static_uuid(f"job_{i}"),
                },
            )

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_different_dataset_interactions(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Tree J -> R -> O0...03, interacting with the same dataset multiple times
    with different operation types:
    J0 -> R0 -> O0, O0 -> D1
    J0 -> R0 -> O1, O1 -> D1
    J0 -> R1 -> O2, O2 -> D1
    """

    operations_per_run = 3
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_location = await builder.create_location(key="different_dataset_interactions_dataset_location")
        dataset = await builder.create_dataset(key="different_dataset_interactions_dataset", location=dataset_location)

        schema = await builder.create_schema(key="different_dataset_interactions_schema")

        # Create a job, run and operation with IO datasets.
        job_type = await builder.create_job_type(key="different_dataset_interactions_job_type")
        job = await builder.create_job(
            key="different_dataset_interactions_job",
            location_key="different_dataset_interactions_job_location",
            job_type=job_type,
        )

        run = await builder.create_run(
            key="different_dataset_interactions_run",
            job=job,
            run_kwargs={
                "job_id": job.id,
                "started_by_user_id": user.id,
                "created_at": created_at + timedelta(seconds=1),
            },
        )

        operations = [
            await builder.create_operation(
                key=f"different_dataset_interactions_operation_{i}",
                run=run,
                operation_kwargs={
                    "run_id": run.id,
                    "created_at": run.created_at + timedelta(seconds=0.2),
                },
            )
            for i in range(operations_per_run)
        ]

        [
            await builder.create_output(
                key=f"different_dataset_interactions_output_{i}",
                operation=operation,
                run=run,
                job=job,
                dataset=dataset,
                output_type=type_,
                schema=schema,
                output_kwargs={
                    "created_at": operation.created_at,
                    "operation_id": operation.id,
                    "run_id": operation.run_id,
                    "job_id": job.id,
                    "dataset_id": dataset.id,
                    "type": type_,
                    "schema_id": schema.id,
                },
            )
            for i, (operation, type_) in enumerate(
                zip(
                    operations,
                    [OutputType.OVERWRITE, OutputType.TRUNCATE, OutputType.DROP],
                    strict=False,
                )
            )
        ]

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_for_long_running_operations(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Three trees of J -> R -> O, but each operation+dataset produces multiple
    inputs/outputs:
    J1 -> R1 -> O1, D1 -> O1 -> D2
    J2 -> R2 -> O2, D2 -> O2 -> D3
    J3 -> R3 -> O3, D3 -> O2 -> D4
    """

    created_at = datetime.now(tz=UTC)
    num_datasets = 4
    num_jobs = 3
    num_io = 10

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_locations = [
            await builder.create_location(
                key=f"long_running_dataset_location_{i}",
                location_kwargs={"type": "hdfs"},
            )
            for i in range(num_datasets)
        ]
        datasets = [
            await builder.create_dataset(
                key=f"long_running_dataset_{i}",
                location=location,
            )
            for i, location in enumerate(dataset_locations)
        ]

        schema = await builder.create_schema(key="long_running_schema")

        # Make graphs
        for i in range(num_jobs):
            job_type = await builder.create_job_type(key=f"long_running_job_type_{i}")
            job = await builder.create_job(
                key=f"long_running_job_{i}",
                location_key=f"long_running_job_location_{i}",
                job_type=job_type,
            )

            run = await builder.create_run(
                key=f"long_running_run_{i}",
                job=job,
                run_kwargs={
                    "job_id": job.id,
                    "started_by_user_id": user.id,
                    "created_at": created_at + timedelta(seconds=i),
                },
            )

            operation = await builder.create_operation(
                key=f"long_running_operation_{i}",
                run=run,
                operation_kwargs={
                    "created_at": run.created_at + timedelta(seconds=0.2),
                    "run_id": run.id,
                },
            )

            for io in range(num_io):
                await builder.create_input(
                    key=f"long_running_input_{i}_{io}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[i],
                    schema=schema,
                    input_kwargs={
                        "created_at": operation.created_at + timedelta(hours=io),
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[i].id,
                        "schema_id": schema.id,
                        "num_files": io,
                        "num_rows": io,
                        "num_bytes": io,
                    },
                )

                await builder.create_output(
                    key=f"long_running_output_{i}_{io}",
                    operation=operation,
                    run=run,
                    job=job,
                    dataset=datasets[i + 1],
                    output_type=OutputType.APPEND,
                    schema=schema,
                    output_kwargs={
                        "created_at": operation.created_at + timedelta(hours=io),
                        "operation_id": operation.id,
                        "run_id": run.id,
                        "job_id": job.id,
                        "dataset_id": datasets[i + 1].id,
                        "type": OutputType.APPEND,
                        "schema_id": schema.id,
                        "num_files": io,
                        "num_rows": io,
                        "num_bytes": io,
                    },
                )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_for_long_running_operations_with_column_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    lineage_for_long_running_operations: LineageResult,
) -> AsyncGenerator[LineageResult, None]:
    """Same as lineage_for_long_running_operations, but with column lineage."""
    lineage = lineage_for_long_running_operations
    num_io = 3

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        for i in range(len(lineage.jobs)):
            # Direct
            await builder.create_column_relation(
                key=f"long_running_column_direct_{i}",
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.AGGREGATION.value,
                    "source_column": "direct_source_column",
                    "target_column": "direct_target_column",
                },
            )
            # Indirect
            await builder.create_column_relation(
                key=f"long_running_column_indirect_{i}",
                fingerprint=generate_static_uuid(f"job_{i}"),
                column_relation_kwargs={
                    "type": DatasetColumnRelationType.JOIN.value,
                    "source_column": "indirect_source_column",
                    "target_column": None,
                },
            )

            for io in range(num_io):
                await builder.create_column_lineage(
                    key=f"long_running_column_lineage_{i}_{io}",
                    operation=lineage.operations[i],
                    run=lineage.runs[i],
                    job=lineage.jobs[i],
                    source_dataset=lineage.datasets[i],
                    target_dataset=lineage.datasets[i + 1],
                    fingerprint=generate_static_uuid(f"job_{i}"),
                    column_lineage_kwargs={
                        "created_at": lineage.operations[i].created_at + timedelta(hours=io),
                    },
                )

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def lineage_with_parent_run_relations(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
) -> AsyncGenerator[LineageResult, None]:
    """
    Lineage with chain of runs: Airflow Task -> Airflow Job -> Spark Job
    R0 -> R1 -> R2
    """
    created_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        builder = LineageBuilder(async_session)
        dataset_location = await builder.create_location(
            key="parent_run_relations_dataset_location",
            location_kwargs={"type": "hdfs"},
        )
        dataset = await builder.create_dataset(
            key="parent_run_relations_dataset",
            location=dataset_location,
        )

        spark_application_job_type = await builder.create_job_type(
            key="parent_run_relations_spark_job_type",
            job_type_kwargs={"type": "SPARK_APPLICATION"},
        )
        airflow_dag_job_type = await builder.create_job_type(
            key="parent_run_relations_airflow_dag_job_type",
            job_type_kwargs={"type": "AIRFLOW_DAG"},
        )
        airflow_task_job_type = await builder.create_job_type(
            key="parent_run_relations_airflow_task_job_type",
            job_type_kwargs={"type": "AIRFLOW_TASK"},
        )

        airflow_dag = await builder.create_job(
            key="parent_run_relations_airflow_dag",
            location_key="parent_run_relations_airflow_location",
            job_type=airflow_dag_job_type,
            job_kwargs={"name": "airflow_dag_name"},
        )
        airflow_task = await builder.create_job(
            key="parent_run_relations_airflow_task",
            location_key="parent_run_relations_airflow_location",
            job_type=airflow_task_job_type,
            job_kwargs={"name": "airflow_task_name", "parent_job_id": airflow_dag.id},
        )
        spark_application = await builder.create_job(
            key="parent_run_relations_spark_application",
            location_key="parent_run_relations_spark_location",
            job_type=spark_application_job_type,
            job_kwargs={"name": "spark_application_name", "parent_job_id": airflow_task.id},
        )

        airflow_dag_run = await builder.create_run(
            key="parent_run_relations_airflow_dag_run",
            job=airflow_dag,
            run_kwargs={
                "job_id": airflow_dag.id,
                "started_by_user_id": user.id,
                "parent_run_id": None,
                "status": RunStatus.STARTED,
                "created_at": created_at + timedelta(seconds=0.3),
                "started_at": created_at + timedelta(seconds=3),
                "ended_at": None,
            },
        )
        airflow_task_run = await builder.create_run(
            key="parent_run_relations_airflow_task_run",
            job=airflow_task,
            run_kwargs={
                "job_id": airflow_task.id,
                "parent_run_id": airflow_dag_run.id,
                "started_by_user_id": user.id,
                "status": RunStatus.FAILED,
                "created_at": created_at + timedelta(seconds=0.4),
                "started_at": created_at + timedelta(seconds=4),
                "ended_at": created_at + timedelta(seconds=240),
            },
        )
        await builder.create_run(
            key="parent_run_relations_spark_run_1",
            job=spark_application,
            run_kwargs={
                "job_id": spark_application.id,
                "parent_run_id": airflow_task_run.id,
                "started_by_user_id": user.id,
                "status": RunStatus.KILLED,
                "created_at": created_at + timedelta(seconds=0.1),
                "started_at": created_at + timedelta(seconds=1),
                "ended_at": created_at + timedelta(seconds=60),
            },
        )
        spark_app_run2 = await builder.create_run(
            key="parent_run_relations_spark_run_2",
            job=spark_application,
            run_kwargs={
                "job_id": spark_application.id,
                "started_by_user_id": user.id,
                "parent_run_id": airflow_task_run.id,
                "status": RunStatus.SUCCEEDED,
                "created_at": created_at + timedelta(seconds=0.2),
                "started_at": created_at + timedelta(seconds=2),
                "ended_at": created_at + timedelta(seconds=120),
            },
        )

        operation = await builder.create_operation(
            key="parent_run_relations_operation",
            run=spark_app_run2,
            operation_kwargs={
                "created_at": spark_app_run2.created_at + timedelta(seconds=0.2),
                "run_id": spark_app_run2.id,
            },
        )

        await builder.create_input(
            key="parent_run_relations_input",
            operation=operation,
            run=spark_app_run2,
            job=spark_application,
            dataset=dataset,
            input_kwargs={
                "created_at": operation.created_at + timedelta(hours=1),
                "operation_id": operation.id,
                "run_id": spark_app_run2.id,
                "job_id": spark_application.id,
                "dataset_id": dataset.id,
            },
        )

        lineage = builder.build()

    yield lineage

    async with async_session_maker() as async_session:
        await clean_db(async_session)
