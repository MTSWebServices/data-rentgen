from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine

    from data_rentgen.db.settings import DatabaseSettings


@contextlib.asynccontextmanager
async def get_async_engine(db_settings: DatabaseSettings) -> AsyncGenerator[AsyncEngine, None]:
    """Create test engine"""
    connection_url = str(db_settings.url)
    engine = create_async_engine(connection_url)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def async_engine(db_settings: DatabaseSettings):
    async with get_async_engine(db_settings) as result:
        yield result
