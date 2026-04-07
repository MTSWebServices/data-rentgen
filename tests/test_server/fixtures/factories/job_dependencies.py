from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Job, JobDependency
from data_rentgen.db.models.dataset_symlink import DatasetSymlinkType
from data_rentgen.utils.uuid import generate_new_uuid
from tests.test_server.fixtures.factories.dataset import create_dataset, make_symlink
from tests.test_server.fixtures.factories.input import create_input
from tests.test_server.fixtures.factories.job import create_job
from tests.test_server.fixtures.factories.job_type import create_job_type
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.fixtures.factories.output import create_output
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture
async def job_dependency_depth_chain(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Job], None]:
    """
    Linear dependency chain of 5 jobs:

        job_1 → job_2 → job_3 → job_4 → job_5

    Each arrow is a JobDependency edge with type "DIRECT_DEPENDENCY".
    Used for testing depth-limited dependency queries.
    """
    async with async_session_maker() as async_session:
        location = await create_location(async_session)
        job_type = await create_job_type(async_session)

        jobs = []
        for i in range(1, 6):
            job = await create_job(
                async_session,
                location_id=location.id,
                job_type_id=job_type.id,
                job_kwargs={"name": f"depth-chain-job-{i}"},
            )
            jobs.append(job)

        async_session.add_all(
            [
                JobDependency(
                    from_job_id=jobs[i].id,
                    to_job_id=jobs[i + 1].id,
                    type="DIRECT_DEPENDENCY",
                )
                for i in range(len(jobs) - 1)
            ],
        )
        await async_session.commit()
        async_session.expunge_all()

    yield jobs

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def job_dependency_chain(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[tuple[tuple[Job, Job, Job], ...], None]:
    """
    Fixture that creates:
    - Parent-child hierarchy: dag -> task -> spark via parent_job_id
    - Job dependency edges: task1 -> task2 and task2 -> task3

    There are no relations like Dag -> Dag and Spark -> Spark.
    """
    async with async_session_maker() as async_session:
        location = await create_location(async_session)

        job_type_dag = await create_job_type(async_session, {"type": "AIRFLOW_DAG"})
        dag1 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_dag.id,
            job_kwargs={"name": "dag1"},
        )
        dag2 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_dag.id,
            job_kwargs={"name": "dag2"},
        )
        dag3 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_dag.id,
            job_kwargs={"name": "dag3"},
        )

        job_type_task = await create_job_type(async_session, {"type": "AIRFLOW_TASK"})
        task1 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_task.id,
            job_kwargs={"name": "task1", "parent_job_id": dag1.id},
        )
        task2 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_task.id,
            job_kwargs={"name": "task2", "parent_job_id": dag2.id},
        )
        task3 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_task.id,
            job_kwargs={"name": "task3", "parent_job_id": dag3.id},
        )

        job_type_spark = await create_job_type(async_session, {"type": "SPARK_APPLICATION"})
        spark1 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "spark1", "parent_job_id": task1.id},
        )
        spark2 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "spark2", "parent_job_id": task2.id},
        )
        spark3 = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "spark3", "parent_job_id": task3.id},
        )

        async_session.add_all(
            [
                JobDependency(from_job_id=task1.id, to_job_id=task2.id, type="DIRECT_DEPENDENCY"),
                JobDependency(from_job_id=task2.id, to_job_id=task3.id, type="DIRECT_DEPENDENCY"),
            ],
        )
        await async_session.commit()
        async_session.expunge_all()

    yield (
        (dag1, dag2, dag3),
        (task1, task2, task3),
        (spark1, spark2, spark3),
    )

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def job_dependency_chain_with_lineage(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    job_dependency_chain: tuple[tuple[Job, Job, Job], ...],
) -> AsyncGenerator[tuple[tuple[Job, Job, Job, Job, Job], ...], None]:
    """
    Extends `job_dependency_chain` with two extra parent-child chains:
    - left:  left_dag  -> left_task  -> left_spark
    - right: right_dag -> right_task -> right_spark

    The chains are connected to the central fixture on task level via IO relations:
    - left_task  -> task1  (inferred via input/output relation)
    - task3      -> right_task (inferred via input/output relation)
    """
    (dag1, dag2, dag3), (task1, task2, task3), (spark1, spark2, spark3) = job_dependency_chain

    async with async_session_maker() as async_session:
        location = await create_location(async_session)
        job_type_dag = await create_job_type(async_session, {"type": "AIRFLOW_DAG"})
        job_type_task = await create_job_type(async_session, {"type": "AIRFLOW_TASK"})
        job_type_spark = await create_job_type(async_session, {"type": "SPARK_APPLICATION"})

        left_dag = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_dag.id,
            job_kwargs={"name": "left_dag"},
        )
        left_task = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_task.id,
            job_kwargs={"name": "left_task", "parent_job_id": left_dag.id},
        )
        left_spark = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "left_spark", "parent_job_id": left_task.id},
        )

        right_dag = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_dag.id,
            job_kwargs={"name": "right_dag"},
        )
        right_task = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_task.id,
            job_kwargs={"name": "right_task", "parent_job_id": right_dag.id},
        )
        right_spark = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "right_spark", "parent_job_id": right_task.id},
        )

        left_dataset_location = await create_location(async_session)
        left_dataset = await create_dataset(async_session, location_id=left_dataset_location.id)
        right_dataset_location = await create_location(async_session)
        right_dataset = await create_dataset(async_session, location_id=right_dataset_location.id)

        # Connect left chain to central chain: left_spark -> spark1
        left_output_created_at = datetime.now(tz=UTC)
        left_input_created_at = left_output_created_at - timedelta(seconds=1)
        await create_output(
            async_session,
            output_kwargs={
                "created_at": left_output_created_at,
                "operation_id": generate_new_uuid(left_output_created_at),
                "run_id": generate_new_uuid(left_output_created_at),
                "job_id": left_spark.id,
                "dataset_id": left_dataset.id,
                "schema_id": None,
            },
        )
        await create_input(
            async_session,
            input_kwargs={
                "created_at": left_input_created_at,
                "operation_id": generate_new_uuid(left_input_created_at),
                "run_id": generate_new_uuid(left_input_created_at),
                "job_id": spark1.id,
                "dataset_id": left_dataset.id,
                "schema_id": None,
            },
        )

        # Connect central chain to right chain: spark3 -> right_spark
        right_output_created_at = datetime.now(tz=UTC) + timedelta(seconds=10)
        right_input_created_at = right_output_created_at - timedelta(seconds=1)
        await create_output(
            async_session,
            output_kwargs={
                "created_at": right_output_created_at,
                "operation_id": generate_new_uuid(right_output_created_at),
                "run_id": generate_new_uuid(right_output_created_at),
                "job_id": spark3.id,
                "dataset_id": right_dataset.id,
                "schema_id": None,
            },
        )
        await create_input(
            async_session,
            input_kwargs={
                "created_at": right_input_created_at,
                "operation_id": generate_new_uuid(right_input_created_at),
                "run_id": generate_new_uuid(right_input_created_at),
                "job_id": right_spark.id,
                "dataset_id": right_dataset.id,
                "schema_id": None,
            },
        )

        async_session.expunge_all()

    yield (
        (left_dag, dag1, dag2, dag3, right_dag),
        (left_task, task1, task2, task3, right_task),
        (left_spark, spark1, spark2, spark3, right_spark),
    )

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def job_dependency_chain_with_lineage_and_symlinks(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[tuple[Job, ...], None]:
    """ "
    Jobs are connected via IO relations with symlinks:
    - left_spark --Out--Symlink--In->spark  (inferred via input/output relation)
    - spark--Out--Symlink--In->right_spark (inferred via input/output relation)
    """

    async with async_session_maker() as async_session:
        location = await create_location(async_session)
        job_type_spark = await create_job_type(async_session, {"type": "SPARK_APPLICATION"})
        left_spark = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "left_spark"},
        )
        spark = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "spark"},
        )
        right_spark = await create_job(
            async_session,
            location_id=location.id,
            job_type_id=job_type_spark.id,
            job_kwargs={"name": "right_spark"},
        )

        # Create datasets connected via symlinks.
        left_dataset_location = await create_location(async_session)
        left_output_dataset = await create_dataset(async_session, location_id=left_dataset_location.id)
        left_input_dataset = await create_dataset(async_session, location_id=left_dataset_location.id)
        await make_symlink(
            async_session=async_session,
            from_dataset=left_output_dataset,
            to_dataset=left_input_dataset,
            type=DatasetSymlinkType.METASTORE,
        )

        right_dataset_location = await create_location(async_session)
        right_output_dataset = await create_dataset(async_session, location_id=right_dataset_location.id)
        right_input_dataset = await create_dataset(async_session, location_id=right_dataset_location.id)
        await make_symlink(
            async_session=async_session,
            from_dataset=right_input_dataset,
            to_dataset=right_output_dataset,
            type=DatasetSymlinkType.WAREHOUSE,
        )

        # Connect left chain to central chain: left_spark -> spark
        left_output_created_at = datetime.now(tz=UTC)
        left_input_created_at = left_output_created_at - timedelta(seconds=1)
        await create_output(
            async_session,
            output_kwargs={
                "created_at": left_output_created_at,
                "operation_id": generate_new_uuid(left_output_created_at),
                "run_id": generate_new_uuid(left_output_created_at),
                "job_id": left_spark.id,
                "dataset_id": left_output_dataset.id,
                "schema_id": None,
            },
        )
        await create_input(
            async_session,
            input_kwargs={
                "created_at": left_input_created_at,
                "operation_id": generate_new_uuid(left_input_created_at),
                "run_id": generate_new_uuid(left_input_created_at),
                "job_id": spark.id,
                "dataset_id": left_input_dataset.id,
                "schema_id": None,
            },
        )

        # Connect central chain to right chain: spark3 -> right_spark
        right_output_created_at = datetime.now(tz=UTC) + timedelta(seconds=10)
        right_input_created_at = right_output_created_at - timedelta(seconds=1)
        await create_output(
            async_session,
            output_kwargs={
                "created_at": right_output_created_at,
                "operation_id": generate_new_uuid(right_output_created_at),
                "run_id": generate_new_uuid(right_output_created_at),
                "job_id": spark.id,
                "dataset_id": right_output_dataset.id,
                "schema_id": None,
            },
        )
        await create_input(
            async_session,
            input_kwargs={
                "created_at": right_input_created_at,
                "operation_id": generate_new_uuid(right_input_created_at),
                "run_id": generate_new_uuid(right_input_created_at),
                "job_id": right_spark.id,
                "dataset_id": right_input_dataset.id,
                "schema_id": None,
            },
        )

        async_session.expunge_all()

    yield (left_spark, spark, right_spark)

    async with async_session_maker() as async_session:
        await clean_db(async_session)
