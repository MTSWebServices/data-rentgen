# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_engine_from_config,
    async_sessionmaker,
)

from data_rentgen.db.settings import DatabaseSettings


def create_session_factory(settings: DatabaseSettings) -> async_sessionmaker[AsyncSession]:
    engine = async_engine_from_config(settings.model_dump(), prefix="")
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def session_generator(settings: DatabaseSettings):
    a_session = create_session_factory(settings)

    async def wrapper() -> AsyncGenerator[AsyncSession, None]:
        async with a_session() as session:
            yield session

    return wrapper
