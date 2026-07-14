from __future__ import annotations

from random import choice, randint
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import ColumnLineage, DatasetColumnRelation, DatasetColumnRelationType
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid, generate_static_uuid
from tests.test_server.fixtures.factories.base import random_string

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession


def column_relation_factory(**kwargs) -> DatasetColumnRelation:
    data = {
        "source_column": random_string(10),
        "target_column": random_string(10),
        "type": choice(list(DatasetColumnRelationType)),
        "fingerprint": generate_static_uuid("fingerprint"),
    }
    data.update(kwargs)
    return DatasetColumnRelation(**data)


async def create_column_relation(
    async_session: AsyncSession,
    fingerprint: UUID,
    column_relation_kwargs: dict | None = None,
) -> DatasetColumnRelation:
    column_relation_kwargs = column_relation_kwargs or {}
    column_relation_kwargs["fingerprint"] = fingerprint
    column_relation_kwargs["target_column"] = column_relation_kwargs.get("target_column") or ""

    column_relation = column_relation_factory(**column_relation_kwargs)
    async_session.add(column_relation)
    await async_session.commit()

    query = select(DatasetColumnRelation).where(
        DatasetColumnRelation.fingerprint == column_relation.fingerprint,
        DatasetColumnRelation.source_column == column_relation.source_column,
        DatasetColumnRelation.target_column == column_relation.target_column,
    )
    return (await async_session.scalars(query)).one()


def column_lineage_factory(**kwargs) -> ColumnLineage:
    created_at = kwargs.pop("created_at", None)
    column_lineage_id = generate_new_uuid(created_at)
    data = {
        "id": column_lineage_id,
        "created_at": created_at or extract_timestamp_from_uuid(column_lineage_id),
        "operation_id": generate_new_uuid(created_at),
        "run_id": generate_new_uuid(created_at),
        "job_id": randint(0, 10000000),
        "source_dataset_id": randint(0, 10000000),
        "target_dataset_id": randint(0, 10000000),
        "fingerprint": generate_static_uuid("fingerprint"),
    }
    data.update(kwargs)
    return ColumnLineage(**data)


async def create_column_lineage(
    async_session: AsyncSession,
    column_lineage_kwargs: dict | None = None,
) -> ColumnLineage:
    column_lineage_kwargs = column_lineage_kwargs or {}
    column_lineage = column_lineage_factory(**column_lineage_kwargs)
    async_session.add(column_lineage)
    await async_session.commit()

    query = (
        select(ColumnLineage)
        .where(ColumnLineage.id == column_lineage.id)
        .options(selectinload(ColumnLineage.dataset_column_relations))
    )
    return await async_session.scalar(query)
