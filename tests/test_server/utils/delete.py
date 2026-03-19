from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import (
    Address,
    Dataset,
    DatasetSymlink,
    DatasetTagValue,
    Input,
    Job,
    JobTagValue,
    Location,
    Operation,
    Output,
    Run,
    Tag,
    TagValue,
)


async def clean_db(async_session: AsyncSession) -> None:
    await async_session.execute(delete(DatasetTagValue))
    await async_session.execute(delete(Location))
    await async_session.execute(delete(Address))
    await async_session.execute(delete(DatasetSymlink))
    await async_session.execute(delete(TagValue))
    await async_session.execute(delete(Tag))
    await async_session.execute(delete(Dataset))
    await async_session.execute(delete(Job))
    await async_session.execute(delete(JobTagValue))
    await async_session.execute(delete(Run))
    await async_session.execute(delete(Input))
    await async_session.execute(delete(Output))
    await async_session.execute(delete(Operation))
    await async_session.commit()
