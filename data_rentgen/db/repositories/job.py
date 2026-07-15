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
    literal,
    select,
    text,
    tuple_,
    union,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Job, JobLastRun, JobTagValue, Location, TagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import JobDTO, JobTypeDTO, PaginationDTO

fetch_bulk_query = select(Job).where(
    tuple_(Job.location_id, Job.name_lower).in_(
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

get_one_query = (
    select(Job)
    .where(
        Job.location_id == bindparam("location_id"),
        Job.name_lower == bindparam("name_lower"),
    )
    .limit(literal(1, literal_execute=True))
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

ancestors_by_job_query = text(
    """
    WITH RECURSIVE ancestors_by_job AS (
        SELECT job.parent_job_id AS parent_job_id, job.id AS child_job_id
        FROM job
        WHERE job.id = ANY(:job_ids) AND job.parent_job_id IS NOT NULL

        UNION

        SELECT parent.parent_job_id, parent.id
        FROM job AS parent
        JOIN ancestors_by_job ON parent.id = ancestors_by_job.parent_job_id
        WHERE parent.parent_job_id IS NOT NULL
    )
    SELECT parent_job_id, child_job_id
    FROM ancestors_by_job
    ORDER BY parent_job_id, child_job_id
    """
)


descendants_by_job_query = text(
    """
    WITH RECURSIVE descendants_by_job AS (
        SELECT job.id AS child_job_id, job.parent_job_id AS parent_job_id
        FROM job
        WHERE job.parent_job_id = ANY(:job_ids)

        UNION

        SELECT child.id, child.parent_job_id
        FROM job AS child
        JOIN descendants_by_job ON child.parent_job_id = descendants_by_job.child_job_id
    )
    SELECT parent_job_id, child_job_id
    FROM descendants_by_job
    ORDER BY parent_job_id, child_job_id
    """
)


class JobRepository(Repository[Job]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        job_ids: list[int],
        parent_job_ids: list[int],
        job_types: list[str],
        tag_value_ids: list[int],
        location_ids: list[int],
        location_types: list[str],
        search_query: str | None,
    ) -> PaginationDTO[Job]:
        where = []
        limit = 0
        if len(job_ids) == 1:
            where.append(Job.id == job_ids[0])  # type: ignore[arg-type]
            limit = 1
        elif job_ids:
            where.append(Job.id == any_(list(job_ids)))  # type: ignore[arg-type]
            limit = len(job_ids)

        if parent_job_ids:
            where.append(Job.parent_job_id == any_(list(parent_job_ids)))  # type: ignore[arg-type]
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
        location_join_clause = Location.id == Job.location_id
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
            order_by = [Job.name_lower]

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
            override_limit=limit,
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
            parent_job_id=job.parent_job.id if job.parent_job else None,
            name=job.name,
            type_id=(job.type or JobTypeDTO.UNKNOWN).id,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def update(self, existing: Job, new: JobDTO) -> Job:
        if new.type and new.type.id:
            existing.type_id = new.type.id
        if new.parent_job:
            existing.parent_job_id = new.parent_job.id

        await self._session.flush([existing])
        if not new.tag_values:
            # in case when jobs have no tag values we can avoid INSERT statements.
            # also parent jobs may have no tag values, so we skip updating them.
            return existing

        # Lock to prevent inserting the same rows from multiple workers
        await self._lock(existing.location_id, existing.name.lower())
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

    async def list_ancestor_relations(self, job_ids: Collection[int]):
        if not job_ids:
            return []
        result = await self._session.execute(ancestors_by_job_query, {"job_ids": list(job_ids)})
        return list(result.all())

    async def list_descendant_relations(self, job_ids: Collection[int]):
        if not job_ids:
            return []
        result = await self._session.execute(descendants_by_job_query, {"job_ids": list(job_ids)})
        return list(result.all())
