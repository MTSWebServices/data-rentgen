from __future__ import annotations

from random import randint
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Input
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def input_factory(**kwargs) -> Input:
    created_at = kwargs.pop("created_at", None)
    input_id = generate_new_uuid(created_at)
    data = {
        "id": input_id,
        "created_at": extract_timestamp_from_uuid(input_id),
        "operation_id": generate_new_uuid(created_at),
        "run_id": generate_new_uuid(created_at),
        "job_id": randint(0, 10000000),
        "dataset_id": randint(0, 10000000),
        "schema_id": None,
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Input(**data)


async def create_input(
    async_session: AsyncSession,
    input_kwargs: dict | None = None,
) -> Input:
    input_kwargs = input_kwargs or {}
    input = input_factory(**input_kwargs)
    async_session.add(input)
    await async_session.commit()

    query = select(Input).where(Input.id == input.id).options(selectinload(Input.schema))
    return await async_session.scalar(query)
