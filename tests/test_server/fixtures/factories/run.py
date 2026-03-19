from __future__ import annotations

from datetime import UTC, datetime, timedelta
from random import choice, randint
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Job, Run, RunStartReason, RunStatus, User
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid
from tests.test_server.fixtures.factories.base import random_datetime, random_string
from tests.test_server.fixtures.factories.job import create_job
from tests.test_server.fixtures.factories.job_type import create_job_type
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.fixtures.factories.user import create_user
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def run_factory(**kwargs):
    created_at = kwargs.pop("created_at", None)
    run_id = generate_new_uuid(created_at)
    data = {
        "created_at": extract_timestamp_from_uuid(run_id),
        "id": run_id,
        "job_id": randint(0, 10000000),
        "parent_run_id": generate_new_uuid(),
        "status": choice(list(RunStatus)),
        "external_id": random_string(128),
        "attempt": random_string(16),
        "persistent_log_url": random_string(32),
        "running_log_url": random_string(32),
        "started_at": random_datetime(),
        "started_by_user_id": randint(0, 10000000),
        "start_reason": choice(list(RunStartReason)),
        "ended_at": random_datetime(),
        "end_reason": random_string(8),
        "expected_start_at": random_datetime(),
    }
    data.update(kwargs)
    return Run(**data)


async def create_run(
    async_session: AsyncSession,
    run_kwargs: dict | None = None,
) -> Run:
    run_kwargs = run_kwargs or {}
    run = run_factory(**run_kwargs)
    async_session.add(run)
    await async_session.commit()
    await async_session.refresh(run)

    return run


@pytest_asyncio.fixture(params=[{}])
async def new_run(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Run, None]:
    params = request.param
    item = run_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def run(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    job: Job,
    user: User,
) -> AsyncGenerator[Run, None]:
    params = request.param
    params.update({"started_by_user_id": user.id})
    params.update({"job_id": job.id})
    async with async_session_maker() as async_session:
        run = await create_run(async_session, run_kwargs=params)

    yield run

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def runs(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
    jobs: list[Job],
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": jobs[i // 2].id,
                    "created_at": started_at + timedelta(seconds=0.1 * i),
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for i in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_job(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    # To be sure runs has different timestamps
                    "created_at": started_at + timedelta(seconds=s),
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for s in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def runs_with_same_parent(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    user: User,
    job: Job,
) -> AsyncGenerator[list[Run], None]:
    size, params = request.param
    started_at = datetime.now(tz=UTC)
    parent_run_id = generate_new_uuid()

    async with async_session_maker() as async_session:
        items = [
            await create_run(
                async_session,
                run_kwargs={
                    "job_id": job.id,
                    "parent_run_id": parent_run_id,
                    # To be sure runs has different timestamps
                    "created_at": started_at + timedelta(seconds=s),
                    "started_by_user_id": user.id,
                    **params,
                },
            )
            for s in range(size)
        ]

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture()
async def runs_search(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[dict[str | None, Run], None]:
    created_at = datetime.now(tz=UTC)
    async with async_session_maker() as async_session:
        spark_location = await create_location(async_session)
        airflow_location = await create_location(async_session)

        spark_user = await create_user(async_session)
        airflow_user = await create_user(async_session)

        spark_application_job_type = await create_job_type(async_session, job_type_kwargs={"type": "SPARK_APPLICATION"})
        airflow_dag_job_type = await create_job_type(async_session, job_type_kwargs={"type": "AIRFLOW_DAG"})
        airflow_task_job_type = await create_job_type(async_session, job_type_kwargs={"type": "AIRFLOW_TASK"})

        spark_application = await create_job(
            async_session,
            location_id=spark_location.id,
            job_type_id=spark_application_job_type.id,
            job_kwargs={"name": "spark_application_name"},
        )
        airflow_dag = await create_job(
            async_session,
            location_id=airflow_location.id,
            job_type_id=airflow_dag_job_type.id,
            job_kwargs={"name": "airflow_dag_name"},
        )
        airflow_task = await create_job(
            async_session,
            location_id=airflow_location.id,
            job_type_id=airflow_task_job_type.id,
            job_kwargs={"name": "airflow_task_name"},
        )

        spark_app_run1 = await create_run(
            async_session,
            run_kwargs={
                "job_id": spark_application.id,
                "started_by_user_id": spark_user.id,
                "external_id": "application_1638922609021_0001",
                "status": RunStatus.KILLED,
                "created_at": created_at + timedelta(seconds=0.1),
                "started_at": created_at + timedelta(seconds=1),
                "ended_at": created_at + timedelta(seconds=60),
            },
        )
        spark_app_run2 = await create_run(
            async_session,
            run_kwargs={
                "job_id": spark_application.id,
                "started_by_user_id": spark_user.id,
                "external_id": "application_1638922609021_0002",
                "status": RunStatus.SUCCEEDED,
                "created_at": created_at + timedelta(seconds=0.2),
                "started_at": created_at + timedelta(seconds=2),
                "ended_at": created_at + timedelta(seconds=120),
            },
        )

        airflow_dag_run1 = await create_run(
            async_session,
            run_kwargs={
                "job_id": airflow_dag.id,
                "started_by_user_id": airflow_user.id,
                "external_id": "dag_0001",
                "status": RunStatus.STARTED,
                "created_at": created_at + timedelta(seconds=0.3),
                "started_at": created_at + timedelta(seconds=3),
                "ended_at": None,
            },
        )
        airflow_task_run1 = await create_run(
            async_session,
            run_kwargs={
                "job_id": airflow_task.id,
                "parent_run_id": airflow_dag_run1.id,
                "started_by_user_id": airflow_user.id,
                "external_id": "task_0001",
                "status": RunStatus.FAILED,
                "created_at": created_at + timedelta(seconds=0.4),
                "started_at": created_at + timedelta(seconds=4),
                "ended_at": created_at + timedelta(seconds=240),
            },
        )

    runs = [
        spark_app_run1,
        spark_app_run2,
        airflow_dag_run1,
        airflow_task_run1,
    ]
    yield {run.external_id: run for run in runs}

    async with async_session_maker() as async_session:
        await clean_db(async_session)
