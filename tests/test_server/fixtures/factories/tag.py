from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Tag
from data_rentgen.db.models.tag_value import TagValue
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def tag_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "name": random_string(32),
    }

    data.update(kwargs)
    return Tag(**data)


async def create_tag(
    async_session: AsyncSession,
    tag_kwargs: dict | None = None,
) -> Tag:
    tag_kwargs = tag_kwargs or {}
    tag = tag_factory(**tag_kwargs)
    del tag.id
    async_session.add(tag)
    await async_session.commit()
    await async_session.refresh(tag)
    return tag


def tag_value_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "value": random_string(32),
        "tag_id": randint(0, 10000000),
    }
    data.update(kwargs)
    return TagValue(**data)


async def create_tag_value(
    async_session: AsyncSession,
    tag_id: int,
    tag_value_kwargs: dict | None = None,
) -> TagValue:
    if tag_value_kwargs:
        tag_value_kwargs.update({"tag_id": tag_id})
    else:
        tag_value_kwargs = {"tag_id": tag_id}
    tag_value = tag_value_factory(**tag_value_kwargs)
    del tag_value.id
    async_session.add(tag_value)
    await async_session.commit()
    await async_session.refresh(tag_value)
    return tag_value


@pytest_asyncio.fixture(params=[{}])
async def new_tag(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Tag, None]:
    params = request.param
    item = tag_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[(3, {})])
async def tags(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Tag], None]:
    size, params = request.param

    items = []
    async with async_session_maker() as async_session:
        for _ in range(size):
            items.append(
                await create_tag(async_session, tag_kwargs=params),
            )
            async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(3, 1, {})])
async def tag_values(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[TagValue], None]:
    values_count, tags_count, value_params = request.param

    items = []
    async with async_session_maker() as async_session:
        tags = [await create_tag(async_session) for _ in range(tags_count)]
        for tag in tags:
            for _ in range(values_count):
                item = await create_tag_value(
                    async_session,
                    tag_id=tag.id,
                    tag_value_kwargs=value_params,
                )
                item.tag = tag
                items.append(item)
            async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def tags_search(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[dict[str, Tag], None]:
    """
    Fixture with explicit tags and tag values for search tests.

    | Tag.name          | TagValue.value                                   |
    |-------------------|--------------------------------------------------|
    | 'company.product' | 'ETL (Department "IT.DataOps")', 'DataPortal'    |
    | 'owner'           | 'team.a', 'team.b'                               |
    | 'environment'     | 'DEV', 'PROD'                                    |
    """
    tag_data = {
        "company.product": ['ETL (Department "IT.DataOps")', "DataPortal"],
        "owner": ["team.a", "team.b"],
        "environment": ["DEV", "PROD"],
    }
    async with async_session_maker() as async_session:
        tags = []
        for tag_name, values in tag_data.items():
            tag = await create_tag(async_session, tag_kwargs={"name": tag_name})
            for value in values:
                item = await create_tag_value(
                    async_session,
                    tag_id=tag.id,
                    tag_value_kwargs={"value": value},
                )
                item.tag = tag
            tags.append(tag)
        async_session.expunge_all()

    yield {tag.name: tag for tag in tags}

    async with async_session_maker() as async_session:
        await clean_db(async_session)
