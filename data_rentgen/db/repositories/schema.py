# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection

from sqlalchemy import any_, bindparam, select

from data_rentgen.db.models import Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import SchemaDTO

# schema JSON can be heavy, avoid loading it if not needed
fetch_bulk_query = select(Schema.digest, Schema.id).where(
    Schema.digest == any_(bindparam("digests")),
)

get_list_by_ids_query = select(Schema).where(Schema.id == any_(bindparam("schema_ids")))
get_one_by_digest_query = select(Schema).where(Schema.digest == bindparam("digest"))


class SchemaRepository(Repository[Schema]):
    async def list_by_ids(self, schema_ids: Collection[int]) -> list[Schema]:
        if not schema_ids:
            return []

        result = await self._session.scalars(get_list_by_ids_query, {"schema_ids": list(schema_ids)})
        return list(result.all())

    async def fetch_known_ids(self, schemas_dto: list[SchemaDTO]) -> list[tuple[SchemaDTO, int | None]]:
        if not schemas_dto:
            return []

        existing = await self._session.execute(
            fetch_bulk_query,
            {
                "digests": [item.digest for item in schemas_dto],
            },
        )
        known_ids = {item.digest: item.id for item in existing.all()}
        return [
            (
                schema_dto,
                known_ids.get(schema_dto.digest),
            )
            for schema_dto in schemas_dto
        ]

    async def create(self, schema: SchemaDTO) -> Schema:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(schema.digest)
        return await self._get(schema) or await self._create(schema)

    async def _get(self, schema: SchemaDTO) -> Schema | None:
        return await self._session.scalar(get_one_by_digest_query, {"digest": schema.digest})

    async def _create(self, schema: SchemaDTO) -> Schema:
        result = Schema(digest=schema.digest, fields=schema.fields)
        self._session.add(result)
        await self._session.flush([result])
        return result
