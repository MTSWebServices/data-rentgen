from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

import pytest_asyncio

from data_rentgen.db.models import Dataset, TagValue
from data_rentgen.db.models.dataset_symlink import DatasetSymlink, DatasetSymlinkType
from tests.test_server.fixtures.factories.base import random_string
from tests.test_server.fixtures.factories.location import create_location
from tests.test_server.utils.delete import clean_db

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from contextlib import AbstractAsyncContextManager

    import pytest
    from sqlalchemy.ext.asyncio import AsyncSession


def dataset_factory(**kwargs):
    data = {
        "id": randint(0, 10000000),
        "location_id": randint(0, 10000000),
        "name": random_string(32),
    }

    data.update(kwargs)
    return Dataset(**data)


async def create_dataset(
    async_session: AsyncSession,
    location_id: int,
    dataset_kwargs: dict | None = None,
    tag_values: set[TagValue] | None = None,
) -> Dataset:
    if dataset_kwargs:
        dataset_kwargs.update({"location_id": location_id})
    else:
        dataset_kwargs = {"location_id": location_id}
    dataset = dataset_factory(**dataset_kwargs)
    del dataset.id
    if tag_values:
        dataset.tag_values |= tag_values
    async_session.add(dataset)
    await async_session.commit()
    await async_session.refresh(dataset)
    return dataset


async def make_symlink(
    async_session: AsyncSession,
    from_dataset: Dataset,
    to_dataset: Dataset,
    type: DatasetSymlinkType,
) -> DatasetSymlink:
    symlink = DatasetSymlink(
        from_dataset_id=from_dataset.id,
        to_dataset_id=to_dataset.id,
        type=type,
    )
    async_session.add(symlink)
    await async_session.commit()
    await async_session.refresh(symlink)
    return symlink


@pytest_asyncio.fixture(params=[{}])
async def new_dataset(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Dataset, None]:
    params = request.param
    item = dataset_factory(**params)

    yield item


@pytest_asyncio.fixture(params=[{}])
async def dataset(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[Dataset, None]:
    params = request.param

    async with async_session_maker() as async_session:
        location = await create_location(async_session)
        item = await create_dataset(async_session, location_id=location.id, dataset_kwargs=params)

        async_session.expunge_all()

    yield item

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def datasets(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[list[Dataset], None]:
    size, params = request.param

    items = []
    async with async_session_maker() as async_session:
        for _ in range(size):
            location = await create_location(async_session)
            items.append(
                await create_dataset(async_session, location_id=location.id, dataset_kwargs=params),
            )
            async_session.expunge_all()

    yield items

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture(params=[(10, {})])
async def datasets_with_symlinks(
    request: pytest.FixtureRequest,
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[tuple[list[Dataset], list[DatasetSymlink]], None]:
    size, params = request.param

    async with async_session_maker() as async_session:
        datasets = []
        for _ in range(size):
            location = await create_location(async_session)
            datasets.append(await create_dataset(async_session, location_id=location.id, dataset_kwargs=params))

        dataset_symlinks = []
        for dataset1, dataset2 in zip(datasets, datasets[1:] + datasets[:1], strict=False):
            # Connect datasets like this
            # Dataset0 - METASTORE - Dataset1
            # Dataset1 - WAREHOUSE - Dataset0
            # ...
            symlink1 = await make_symlink(
                async_session=async_session,
                from_dataset=dataset1,
                to_dataset=dataset2,
                type=DatasetSymlinkType.METASTORE,
            )
            symlink2 = await make_symlink(
                async_session=async_session,
                from_dataset=dataset2,
                to_dataset=dataset1,
                type=DatasetSymlinkType.WAREHOUSE,
            )
            dataset_symlinks.extend([symlink1, symlink2])

        async_session.expunge_all()

    yield datasets, dataset_symlinks

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def datasets_search(
    async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]],
) -> AsyncGenerator[tuple[dict[str, Dataset], dict[str, Dataset], dict[str, Dataset]], None]:
    """
    Fixture with explicit dataset, locations names and addresses urls for search tests.
    The fixtures create database structure like this:
    |Dataset ID | Dataset.name                       | Location.name               | Location.type | Address.url                        |
    |---------- | ---------------------------------- | --------------------------- | ------------- | ---------------------------------- |
    |0          | 'postgres.public.history'          | 'random-location-name'      | 'random'      | 'random-url'                       |
    |0          | 'postgres.public.history'          | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'postgres.public.location_history' | 'random-location-name'      | 'random'      | 'random-url'                       |
    |1          | 'postgres.public.location_history' | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | '/user/hive/warehouse/transfers'   | 'random-location-name'      | 'random'      | 'random-url'                       |
    |2          | '/user/hive/warehouse/transfers'   | 'random-location-name'      | 'random'      | 'random-url'                       |
    |3          | 'random-dataset-name'              | 'postgres.location'         | 'postgres'    | 'random-url'                       |
    |3          | 'random-dataset-name'              | 'postgres.location'         | 'postgres'    | 'random-url'                       |
    |4          | 'random-dataset-name'              | 'postgres.history_location' | 'postgres'    | 'random-url'                       |
    |4          | 'random-dataset-name'              | 'postgres.history_location' | 'postgres'    | 'random-url'                       |
    |5          | 'random-dataset-name'              | 'my-cluster'                | 'hdfs'        | 'random-url'                       |
    |5          | 'random-dataset-name'              | 'my-cluster'                | 'hdfs'        | 'random-url'                       |
    |6          | 'random-dataset-name'              | 'with-external-id'          | 'random'      | 'random-url'                       |
    |6          | 'random-dataset-name'              | 'with-external-id'          | 'random'      | 'random-url'                       |
    |7          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://my-postgres-host:8012'     |
    |7          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://my-postgres-host:2108'     |
    |8          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://your-postgres-host:2108'   |
    |8          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'http://your-postgres-host:8012'   |
    |9          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'hdfs://my-cluster-namenode:2080'  |
    |9          | 'random-dataset-name'              | 'random-location-name'      | 'random'      | 'hdfs://my-cluster-namenode:8020'  |

    Every location relate to two dataset and two addresses. 2-1-2
    tip: you can imagine it like identity matrix with not-random names on diagonal.
    """
    location_kwargs = [
        {"name": "postgres.location", "type": "postgres", "external_id": None},
        {"name": "postgres.history_location", "type": "postgres", "external_id": None},
        {"name": "my-cluster", "type": "hdfs", "external_id": None},
        {"name": "with-external-id", "type": "random", "external_id": "abc123"},
    ]
    address_kwargs = [
        {"urls": ["http://my-postgres-host:8012", "http://my-postgres-host:2108"]},
        {"urls": ["http://your-postgres-host:8012", "http://your-postgres-host:2108"]},
        {"urls": ["hdfs://my-cluster-namenode:8020", "hdfs://my-cluster-namenode:2080"]},
    ]
    dataset_kwargs = [
        {"name": "postgres.public.history"},
        {"name": "postgres.public.location_history"},
        {"name": "/user/hive/warehouse/transfers"},
    ]
    addresses_url = [url for address in address_kwargs for url in address["urls"]]
    async with async_session_maker() as async_session:
        locations_with_name = [
            await create_location(
                async_session,
                location_kwargs=kwargs,
                address_kwargs={"urls": [random_string(32), random_string(32)]},  # Each location has 2 addresses
            )
            for kwargs in location_kwargs
        ]
        locations_with_address_urls = [
            await create_location(
                async_session,
                address_kwargs=kwargs,
            )
            for kwargs in address_kwargs
        ]
        random_location = [
            await create_location(
                async_session,
                location_kwargs={"type": "random"},
                address_kwargs={"urls": [random_string(32), random_string(32)]},
            )
            for _ in range(3)
        ]
        datasets_with_name = [
            await create_dataset(
                async_session,
                location_id=random_location[i].id,
                dataset_kwargs=kwargs,
            )
            for i, kwargs in enumerate(dataset_kwargs)
        ]
        datasets_with_location_name = [
            await create_dataset(
                async_session,
                location_id=location.id,
            )
            for location in locations_with_name
        ]
        datasets_with_address_urls = [
            await create_dataset(
                async_session,
                location_id=location.id,
            )
            for location in locations_with_address_urls
        ]

        async_session.expunge_all()

    datasets_by_name = {dataset.name: dataset for dataset in datasets_with_name}
    datasets_by_location = dict(
        zip([location.name for location in locations_with_name], datasets_with_location_name, strict=False),
    )
    datasets_by_address = dict(
        zip(addresses_url, [dataset for dataset in datasets_with_address_urls for _ in range(2)], strict=False),
    )

    yield datasets_by_name, datasets_by_location, datasets_by_address

    async with async_session_maker() as async_session:
        await clean_db(async_session)


@pytest_asyncio.fixture
async def make_dataset(async_session_maker: Callable[[], AbstractAsyncContextManager[AsyncSession]]):
    async def _create(tag_values: list[TagValue] | None = None, dataset_kwargs: dict | None = None) -> Dataset:
        async with async_session_maker() as async_session:
            location = await create_location(async_session)
            dataset = await create_dataset(
                async_session,
                location_id=location.id,
                dataset_kwargs=dataset_kwargs,
                tag_values=set(tag_values or []),
            )
            async_session.expunge_all()
            return dataset

    yield _create

    async with async_session_maker() as async_session:
        await clean_db(async_session)
