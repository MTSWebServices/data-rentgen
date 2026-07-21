from __future__ import annotations

from random import choice, randint
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Output, OutputType
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, generate_new_uuid

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def output_factory(**kwargs) -> Output:
    created_at = kwargs.pop("created_at", None)
    output_id = generate_new_uuid(created_at)
    data = {
        "id": output_id,
        "created_at": extract_timestamp_from_uuid(output_id),
        "operation_id": generate_new_uuid(created_at),
        "run_id": generate_new_uuid(created_at),
        "job_id": randint(0, 10000000),
        "dataset_id": randint(0, 10000000),
        "type": choice(list(OutputType)),
        "schema_id": None,
        "num_bytes": randint(0, 10000000),
        "num_rows": randint(0, 10000),
        "num_files": randint(0, 100),
    }
    data.update(kwargs)
    return Output(**data)


async def create_output(
    async_session: AsyncSession,
    output_kwargs: dict | None = None,
) -> Output:
    output_kwargs = output_kwargs or {}
    output = output_factory(**output_kwargs)
    async_session.add(output)
    await async_session.commit()

    query = select(Output).where(Output.id == output.id).options(selectinload(Output.schema))
    return await async_session.scalar(query)
