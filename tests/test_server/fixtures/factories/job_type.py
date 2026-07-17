from __future__ import annotations

from random import choice, randint
from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy import delete, select

from data_rentgen.db.models import JobType
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def job_type_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "type": choice(["SPARK_APPLICATION", "AIRFLOW_DAG", "AIRFLOW_TASK", "UNKNOWN_SOMETHING"]),
    }
    data.update(kwargs)
    return JobType(**data)


async def create_job_type(
    async_session: AsyncSession,
    job_type_kwargs: dict | None = None,
) -> JobType:
    job_type_kwargs = job_type_kwargs or {}
    job_type = job_type_factory(**job_type_kwargs)
    del job_type.id
    job_type_real = await async_session.scalar(select(JobType).where(JobType.type == job_type.type))
    if job_type_real:
        return job_type_real

    async_session.add(job_type)
    await async_session.commit()
    await async_session.refresh(job_type)
    return job_type


@pytest_asyncio.fixture(params=[{}])
async def new_job_type(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[JobType, None]:
    params = request.param
    item = job_type_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def job_type(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[JobType, None]:
    params = request.param

    async with async_session_maker() as async_session:
        item = await create_job_type(async_session, job_type_kwargs=params)

        async_session.expunge_all()

    yield item

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(5, {})])
async def job_types(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[JobType], None]:
    size, params = request.param

    async with async_session_maker() as async_session:
        await async_session.execute(delete(JobType))

        items = []
        for _ in range(size):
            item = await create_job_type(async_session, **params)
            items.append(item)

        async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)
