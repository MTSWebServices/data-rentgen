# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection

from sqlalchemy import (
    ARRAY,
    ColumnElement,
    CompoundSelect,
    Integer,
    Row,
    Select,
    SQLColumnExpression,
    String,
    any_,
    asc,
    bindparam,
    cast,
    delete,
    desc,
    distinct,
    func,
    select,
    tuple_,
    union,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Job, JobLastRun, JobTagValue, Location, TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import JobDTO, PaginationDTO

UNKNOWN_JOB_TYPE = 0


fetch_bulk_query = select(Job).where(
    tuple_(Job.location_id, func.lower(Job.name)).in_(
        select(
            func.unnest(
                cast(bindparam("location_ids"), ARRAY(Integer())),
                cast(bindparam("names_lower"), ARRAY(String())),
            )
            .table_valued("location_id", "name_lower")
            .render_derived(),
        ),
    ),
)

get_one_query = select(Job).where(
    Job.location_id == bindparam("location_id"),
    func.lower(Job.name) == bindparam("name_lower"),
)

get_list_query = (
    select(Job)
    .where(
        Job.id == any_(bindparam("job_ids")),
    )
    .options(selectinload(Job.location).selectinload(Location.addresses))
)

get_stats_query = (
    select(
        Job.location_id.label("location_id"),
        func.count(Job.id.distinct()).label("total_jobs"),
    )
    .where(
        Job.location_id == any_(bindparam("location_ids")),
    )
    .group_by(Job.location_id)
)

update_job_type_query = update(Job).where(Job.id == bindparam("job_id")).values(type_id=bindparam("type_id"))

insert_tag_value_query = (
    insert(JobTagValue)
    .values(
        {
            "job_id": bindparam("job_id"),
            "tag_value_id": bindparam("tag_value_id"),
        }
    )
    .on_conflict_do_nothing(index_elements=["job_id", "tag_value_id"])
)
delete_tag_value_query = delete(JobTagValue).where(
    JobTagValue.c.job_id == bindparam("job_id"),
    ~(JobTagValue.c.tag_value_id == any_(bindparam("tag_value_ids"))),
)


class JobRepository(Repository[Job]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: Collection[int],
        job_types: Collection[str],
        tag_value_ids: Collection[int],
        location_ids: Collection[int],
        location_types: Collection[str],
        search_query: str | None,
    ) -> PaginationDTO[Job]:
        where = []
        location_join_clause = Location.id == Job.location_id
        if job_ids:
            where.append(Job.id == any_(list(job_ids)))  # type: ignore[arg-type]
        if job_types:
            where.append(Job.type == any_(list(job_types)))  # type: ignore[arg-type]
        if location_ids:
            where.append(Job.location_id == any_(list(location_ids)))  # type: ignore[arg-type]
        if location_types:
            location_type_lower = [location_type.lower() for location_type in location_types]
            where.append(Location.type == any_(location_type_lower))  # type: ignore[arg-type]

        if tag_value_ids:
            tv_ids = list(tag_value_ids)
            job_ids_subq = (
                select(Job.id)
                .join(Job.tag_values)
                .where(TagValue.id.in_(tv_ids))
                .group_by(Job.id)
                # If multiple tag values are passed, job should have both of them (AND, not OR)
                .having(func.count(distinct(TagValue.id)) == len(tv_ids))
            )
            where.append(Job.id.in_(job_ids_subq))

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            job_stmt = (
                select(Job, ts_rank(Job.search_vector, tsquery).label("search_rank"))
                .join(Location, location_join_clause)
                .where(ts_match(Job.search_vector, tsquery), *where)
            )
            location_stmt = (
                select(Job, ts_rank(Location.search_vector, tsquery).label("search_rank"))
                .join(Location, location_join_clause)
                .where(ts_match(Location.search_vector, tsquery), *where)
            )
            address_stmt = (
                select(Job, func.max(ts_rank(Address.search_vector, tsquery).label("search_rank")))
                .join(Location, location_join_clause)
                .join(Address, Address.location_id == Job.location_id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Job.id, Location.id, Address.id)
            )

            union_cte = union(job_stmt, location_stmt, address_stmt).cte()

            job_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *job_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*job_columns)
            order_by = [desc("search_rank"), asc("name")]
        else:
            query = select(Job).join(Location, location_join_clause).where(*where)
            order_by = [Job.name]

        options = [
            selectinload(Job.location).selectinload(Location.addresses),
            selectinload(Job.tag_values).selectinload(TagValue.tag),
            selectinload(Job.last_run).selectinload(JobLastRun.started_by_user),  # type: ignore[attr-defined]
        ]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def fetch_bulk(self, jobs_dto: list[JobDTO]) -> list[tuple[JobDTO, Job | None]]:
        if not jobs_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "location_ids": [item.location.id for item in jobs_dto],
                "names_lower": [item.name.lower() for item in jobs_dto],
            },
        )
        existing = {(job.location_id, job.name.lower()): job for job in scalars.all()}
        return [
            (
                job_dto,
                existing.get((job_dto.location.id, job_dto.name.lower())),  # type: ignore[arg-type]
            )
            for job_dto in jobs_dto
        ]

    async def create_or_update(self, job: JobDTO) -> Job:
        result = await self._get(job)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(job.location.id, job.name.lower())
            result = await self._get(job)

        if not result:
            result = await self._create(job)
        return await self.update(result, job)

    async def _get(self, job: JobDTO) -> Job | None:
        return await self._session.scalar(
            get_one_query,
            {
                "location_id": job.location.id,
                "name_lower": job.name.lower(),
            },
        )

    async def _create(self, job: JobDTO) -> Job:
        result = Job(
            location_id=job.location.id,
            name=job.name,
            type_id=job.type.id if job.type else UNKNOWN_JOB_TYPE,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def update(self, existing: Job, new: JobDTO) -> Job:
        # almost of fields are immutable, so we can avoid UPDATE statements if row is unchanged
        if new.type and new.type.id and existing.type_id != new.type.id:
            await self._session.execute(
                update_job_type_query,
                {
                    "job_id": existing.id,
                    "type_id": new.type.id,
                },
            )

        if not new.tag_values:
            # in case when jobs have no tag values we can avoid INSERT statements.
            # also parent jobs may have no tag values, so we skip updating them.
            return existing

        # Lock to prevent inserting the same rows from multiple workers
        await self._lock(existing.location_id, existing.name)
        await self._session.execute(
            insert_tag_value_query,
            [
                {
                    "job_id": existing.id,
                    "tag_value_id": tag_value_dto.id,
                }
                for tag_value_dto in new.tag_values
            ],
        )

        # To avoid accumulating too many tag values,
        # e.g. upgrading version of Airflow/Spark/OL/etc will keep both old and new version tags,
        # we keep only tags for the most recent job run.
        await self._session.execute(
            delete_tag_value_query,
            {"job_id": existing.id, "tag_value_ids": [tag_value_dto.id for tag_value_dto in new.tag_values]},
        )
        return existing

    async def list_by_ids(self, job_ids: Collection[int]) -> list[Job]:
        if not job_ids:
            return []

        result = await self._session.scalars(get_list_query, {"job_ids": list(job_ids)})
        return list(result.all())

    async def get_stats_by_location_ids(self, location_ids: Collection[int]) -> dict[int, Row]:
        if not location_ids:
            return {}

        query_result = await self._session.execute(get_stats_query, {"location_ids": list(location_ids)})
        return {row.location_id: row for row in query_result.all()}
