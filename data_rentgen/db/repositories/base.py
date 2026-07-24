# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from hashlib import sha1
from typing import Any, Generic, TypeVar

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    ScalarResult,
    Select,
    SQLColumnExpression,
    bindparam,
    func,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import ExecutableOption

from data_rentgen.db.models import Base
from data_rentgen.dto import PaginationDTO

Model = TypeVar("Model", bound=Base)

advisory_lock_statement = select(func.pg_advisory_xact_lock(bindparam("key")))


class Repository(ABC, Generic[Model]):
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self._session = session

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}()"

    @classmethod
    def model_type(cls) -> type[Model]:
        # Get `User` from `UserRepository(Repository[User])`
        return cls.__orig_bases__[0].__args__[0]  # type: ignore[attr-defined]

    async def _paginate_by_query(
        self,
        *,
        query: Select | CompoundSelect,
        order_by: Sequence[ColumnElement | SQLColumnExpression],
        page: int,
        page_size: int,
        override_limit: int = 0,
        options: Sequence[ExecutableOption] | None = None,
    ) -> PaginationDTO[Model]:
        model_type = self.model_type()
        limit = min(page_size, override_limit) if override_limit else page_size
        items_query = select(model_type).from_statement(
            query.order_by(*order_by).limit(limit).offset((page - 1) * page_size),
        )
        if options:
            items_query = items_query.options(*options)

        total_query = select(func.count()).select_from(query.subquery())
        items_result: ScalarResult[Model] = await self._session.scalars(items_query)

        total_count: int = await self._session.scalar(total_query)  # type: ignore[assignment]
        return PaginationDTO[model_type](  # type: ignore[valid-type]
            items=list(items_result.all()),
            total_count=total_count,
            page=page,
            page_size=page_size,
        )

    async def _lock(
        self,
        *keys: Any,
    ) -> None:
        """
        Take a lock on a specific table and set of keys, to avoid inserting the same row multiple times.
        Based on [pg_advisory_xact_lock](https://www.postgresql.org/docs/current/functions-admin.html).
        Lock is held until the transaction is committed or rolled back.
        """
        model_type = self.model_type()
        data = ".".join(map(str, [model_type.__table__, *keys]))
        digest = sha1(data.encode("utf-8"), usedforsecurity=False).digest()
        # sha1 returns 160bit hash, we need only first 64 bits
        lock_key = int.from_bytes(digest[:8], byteorder="big", signed=True)
        await self._session.execute(advisory_lock_statement, {"key": lock_key})
