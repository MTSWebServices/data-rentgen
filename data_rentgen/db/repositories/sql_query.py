# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import any_, bindparam, literal, select

from data_rentgen.db.models.sql_query import SQLQuery
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SQLQueryDTO

# SQLQuery text can be heavy, avoid loading it if not needed
fetch_bulk_query = select(SQLQuery.fingerprint, SQLQuery.id).where(
    SQLQuery.fingerprint == any_(bindparam("fingerprints")),
)

get_one_by_fingerprint_query = (
    select(SQLQuery).where(SQLQuery.fingerprint == bindparam("fingerprint")).limit(literal(1, literal_execute=True))
)


class SQLQueryRepository(Repository[SQLQuery]):
    async def fetch_known_ids(self, sql_queries_dto: list[SQLQueryDTO]) -> list[tuple[SQLQueryDTO, int | None]]:
        if not sql_queries_dto:
            return []

        existing = await self._session.execute(
            fetch_bulk_query,
            {
                "fingerprints": [item.fingerprint for item in sql_queries_dto],
            },
        )
        known_ids = {item.fingerprint: item.id for item in existing.all()}
        return [
            (
                sql_query_dto,
                known_ids.get(sql_query_dto.fingerprint),
            )
            for sql_query_dto in sql_queries_dto
        ]

    async def create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(sql_query.fingerprint)
        return await self._get(sql_query) or await self._create(sql_query)

    async def _get(self, sql_query: SQLQueryDTO) -> SQLQuery | None:
        return await self._session.scalar(
            get_one_by_fingerprint_query,
            {"fingerprint": sql_query.fingerprint},
        )

    async def _create(self, sql_query: SQLQueryDTO) -> SQLQuery:
        result = SQLQuery(fingerprint=sql_query.fingerprint, query=sql_query.query)
        self._session.add(result)
        await self._session.flush([result])
        return result
