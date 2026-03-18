# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    ColumnElement,
    any_,
    bindparam,
    column,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased, selectinload

from data_rentgen.db.models import Job, Location, Run, RunStartReason, RunStatus, User
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import PaginationDTO, RunDTO
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, get_max_uuid, get_min_uuid

# Do not use `tuple_(Run.created_at, Run.id).in_(...),
# as this is too complex filter for Postgres to make an optimal query plan.
# Primary key starts with id already, and created_at filter is used to select specific partitions
get_list_by_id_query = (
    select(Run)
    .where(
        Run.created_at >= bindparam("since"),
        Run.created_at <= bindparam("until"),
        Run.id == any_(bindparam("run_ids")),
    )
    .options(selectinload(Run.job).joinedload(Job.location).selectinload(Location.addresses))
    .options(selectinload(Run.started_by_user))
)

get_list_by_job_ids_query = (
    select(Run)
    .where(
        Run.id >= bindparam("min_id"),
        Run.created_at >= bindparam("since"),
        Run.job_id == any_(bindparam("job_ids")),
    )
    .options(selectinload(Run.job).joinedload(Job.location).selectinload(Location.addresses))
    .options(selectinload(Run.started_by_user))
)

fetch_bulk_query = select(Run).where(
    Run.created_at >= bindparam("since"),
    Run.id == any_(bindparam("run_ids")),
)

insert_statement = insert(Run).values(
    created_at=bindparam("created_at"),
    id=bindparam("id"),
    job_id=bindparam("job_id"),
    status=bindparam("status"),
    parent_run_id=bindparam("parent_run_id"),
    started_at=bindparam("started_at"),
    started_by_user_id=bindparam("started_by_user_id"),
    start_reason=bindparam("start_reason"),
    ended_at=bindparam("ended_at"),
    expected_start_at=bindparam("expected_start_at"),
    expected_end_at=bindparam("expected_end_at"),
)
inserted_row = insert_statement.excluded
insert_statement = insert_statement.on_conflict_do_update(
    index_elements=[Run.created_at, Run.id],
    set_={
        "job_id": inserted_row.job_id,
        "status": func.greatest(inserted_row.status, Run.status),
        "parent_run_id": func.coalesce(inserted_row.parent_run_id, Run.parent_run_id),
        "started_at": func.coalesce(inserted_row.started_at, Run.started_at),
        "started_by_user_id": func.coalesce(inserted_row.started_by_user_id, Run.started_by_user_id),
        "start_reason": func.coalesce(inserted_row.start_reason, Run.start_reason),
        "ended_at": func.coalesce(inserted_row.ended_at, Run.ended_at),
        "external_id": func.coalesce(inserted_row.external_id, Run.external_id),
        "attempt": func.coalesce(inserted_row.attempt, Run.attempt),
        "persistent_log_url": func.coalesce(inserted_row.persistent_log_url, Run.persistent_log_url),
        "running_log_url": func.coalesce(inserted_row.running_log_url, Run.running_log_url),
        "expected_start_at": func.coalesce(inserted_row.expected_start_at, Run.expected_start_at),
        "expected_end_at": func.coalesce(inserted_row.expected_end_at, Run.expected_end_at),
    },
)


child_run = aliased(Run, name="child")
parent_run = aliased(Run, name="parent")

ancestors_by_run_base_part = (
    select(
        child_run.id.label("child_run_id"),
        parent_run.id.label("parent_run_id"),
    )
    .select_from(child_run)
    .join(parent_run, child_run.parent_run_id == parent_run.id)
    .where(
        child_run.id == any_(bindparam("run_ids")),
        child_run.created_at >= bindparam("since"),
        child_run.created_at <= bindparam("until"),
    )
)
ancestors_by_run_cte = ancestors_by_run_base_part.cte("ancestors_by_run", recursive=True)

ancestors_by_run_recursive_part = (
    select(
        child_run.id.label("child_run_id"),
        parent_run.id.label("parent_run_id"),
    )
    .select_from(child_run)
    .join(parent_run, child_run.parent_run_id == parent_run.id)
    .where(
        child_run.id == ancestors_by_run_cte.c.parent_run_id,
    )
)
ancestors_by_run_cte = ancestors_by_run_cte.union(ancestors_by_run_recursive_part)

descendants_by_run_base_part = (
    select(
        parent_run.id.label("parent_run_id"),
        child_run.id.label("child_run_id"),
    )
    .select_from(parent_run)
    .join(child_run, child_run.parent_run_id == parent_run.id)
    .where(
        parent_run.id == any_(bindparam("run_ids")),
        parent_run.created_at >= bindparam("since"),
        parent_run.created_at <= bindparam("until"),
    )
)
descendants_by_run_cte = descendants_by_run_base_part.cte("descendants_by_run", recursive=True)

descendants_by_run_recursive_part = (
    select(
        parent_run.id.label("parent_run_id"),
        child_run.id.label("child_run_id"),
    )
    .select_from(parent_run)
    .join(child_run, child_run.parent_run_id == parent_run.id)
    .where(
        parent_run.id == descendants_by_run_cte.c.child_run_id,
    )
)
descendants_by_run_cte = descendants_by_run_cte.union(descendants_by_run_recursive_part)


class RunRepository(Repository[Run]):
    async def paginate(  # noqa: PLR0912, C901, PLR0915
        self,
        page: int,
        page_size: int,
        since: datetime | None,
        until: datetime | None,
        run_ids: Collection[UUID],
        parent_run_ids: Collection[UUID],
        job_ids: Collection[int],
        job_types: Collection[str],
        job_location_ids: Collection[int],
        statuses: Collection[RunStatus],
        started_by_users: Collection[str],
        started_since: datetime | None,
        started_until: datetime | None,
        ended_since: datetime | None,
        ended_until: datetime | None,
        search_query: str | None,
    ) -> PaginationDTO[Run]:
        # do not use `tuple_(Run.created_at, Run.id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        where = []

        # created_at and id are always correlated,
        # and primary key starts with id, so we need to apply filter on both
        # to get the most optimal query plan
        if run_ids:
            min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
            max_run_created_at = extract_timestamp_from_uuid(max(run_ids))
            # narrow created_at range
            min_created_at = max(filter(None, [since, min_run_created_at]))
            max_created_at = min(filter(None, [until, max_run_created_at]))
            where = [
                Run.created_at >= min_created_at,
                Run.created_at <= max_created_at,
                Run.id == any_(list(run_ids)),  # type: ignore[arg-type]
            ]
        else:
            if since:
                where = [
                    Run.created_at >= since,
                    Run.id >= get_min_uuid(since),
                ]

            if until:
                where += [
                    Run.created_at <= until,
                    Run.id <= get_max_uuid(until),
                ]

        if job_ids:
            where.append(Run.job_id == any_(list(job_ids)))  # type: ignore[arg-type]
        if parent_run_ids:
            where.append(Run.parent_run_id == any_(list(parent_run_ids)))  # type: ignore[arg-type]
        if statuses:
            where.append(Run.status == any_(statuses))  # type: ignore[arg-type]
        if started_since:
            where.append(Run.started_at >= started_since)
        if started_until:
            where.append(Run.started_at <= started_until)
        if ended_since:
            where.append(Run.ended_at >= ended_since)
        if ended_until:
            where.append(Run.ended_at <= ended_until)

        run = select(Run).where(*where)

        if job_types or job_location_ids:
            job = aliased(Job, name="job_type")
            run = run.join(job, Run.job_id == job.id)
            if job_types:
                where.append(job.type == any_(list(job_types)))  # type: ignore[arg-type]
            if job_location_ids:
                where.append(job.location_id == any_(list(job_location_ids)))  # type: ignore[arg-type]

        if started_by_users:
            usernames_lower = [name.lower() for name in started_by_users]
            run = run.join(User, Run.started_by_user_id == User.id)
            where.append(func.lower(User.name) == any_(usernames_lower))  # type: ignore[arg-type]

        query = run.where(*where)
        order_by: list[ColumnElement] = [desc("created_at"), desc("id")]
        if search_query:
            tsquery = make_tsquery(search_query)

            run_cte = query.cte()
            run_stmt = select(run_cte, ts_rank(column("search_vector"), tsquery).label("search_rank")).where(
                ts_match(column("search_vector"), tsquery),
            )
            job_search = aliased(Job, name="job_search")
            job_stmt = (
                select(run_cte, ts_rank(job_search.search_vector, tsquery).label("search_rank"))
                .join(job_search, job_search.id == column("job_id"))
                .where(ts_match(job_search.search_vector, tsquery))
            )

            union_cte = union(run_stmt, job_stmt).cte()

            run_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *run_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*run_columns)
            # place the most recent runs on top
            order_by = [desc("search_rank"), desc("created_at"), desc("id")]

        options = [
            selectinload(Run.job).joinedload(Job.location).selectinload(Location.addresses),
            selectinload(Run.started_by_user),
        ]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def list_by_ids(self, run_ids: Collection[UUID]) -> list[Run]:
        if not run_ids:
            return []

        result = await self._session.scalars(
            get_list_by_id_query,
            {
                "since": extract_timestamp_from_uuid(min(run_ids)),
                "until": extract_timestamp_from_uuid(max(run_ids)),
                "run_ids": list(run_ids),
            },
        )
        return list(result.all())

    async def list_by_job_ids(self, job_ids: Collection[int], since: datetime, until: datetime | None) -> list[Run]:
        if not job_ids:
            return []

        query = get_list_by_job_ids_query
        if until:
            # until is rarely used, avoid making query too complicated
            query = query.where(Run.created_at <= until)

        result = await self._session.scalars(
            query,
            {
                "min_id": get_min_uuid(since),
                "since": since,
                "job_ids": list(job_ids),
            },
        )
        return list(result.all())

    async def fetch_bulk(self, runs_dto: list[RunDTO]) -> list[tuple[RunDTO, Run | None]]:
        if not runs_dto:
            return []

        ids = [run_dto.id for run_dto in runs_dto]
        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "since": extract_timestamp_from_uuid(min(ids)),
                "run_ids": ids,
            },
        )
        existing = {run.id: run for run in scalars.all()}
        return [(run_dto, existing.get(run_dto.id)) for run_dto in runs_dto]

    async def create_or_update(self, run: RunDTO) -> Run:
        result = await self._get(run)
        if not result:
            return await self.create(run)
        return await self.update(result, run)

    async def _get(self, run: RunDTO) -> Run | None:
        query = select(Run).where(Run.id == run.id, Run.created_at == run.created_at)
        return await self._session.scalar(query)

    async def create(self, run: RunDTO) -> Run:
        result = Run(
            created_at=run.created_at,
            id=run.id,
            job_id=run.job.id,
            status=RunStatus(run.status),
            parent_run_id=run.parent_run.id if run.parent_run else None,
            started_at=run.started_at,
            started_by_user_id=run.user.id if run.user else None,
            start_reason=RunStartReason(run.start_reason) if run.start_reason else None,
            ended_at=run.ended_at,
            external_id=run.external_id,
            attempt=run.attempt,
            persistent_log_url=run.persistent_log_url,
            running_log_url=run.running_log_url,
            expected_start_at=run.expected_start_at,
            expected_end_at=run.expected_end_at,
        )
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def update(
        self,
        existing: Run,
        new: RunDTO,
    ) -> Run:
        # for parent_run most of fields are None, so we can avoid UPDATE statements if row is unchanged
        optional_fields = {
            # Workaround for https://github.com/OpenLineage/OpenLineage/issues/3846.
            # In some cases, Spark STARTED event may be send from "unknown" job,
            # but sequential RUNNING and STOPPED events are send with proper job name. Bound run to proper job.
            "job_id": new.job.id or existing.job_id,
            # Merge new information with existing one
            "status": max(RunStatus(new.status), existing.status),
            "parent_run_id": new.parent_run.id if new.parent_run else None,
            "started_at": new.started_at,
            "started_by_user_id": new.user.id if new.user else None,
            "start_reason": RunStartReason(new.start_reason) if new.start_reason else None,
            "ended_at": new.ended_at,
            "external_id": new.external_id,
            "attempt": new.attempt,
            "persistent_log_url": new.persistent_log_url,
            "running_log_url": new.running_log_url,
            "expected_start_at": new.expected_start_at,
            "expected_end_at": new.expected_end_at,
        }
        for col_name, value in optional_fields.items():
            if value is not None:
                setattr(existing, col_name, value)

        await self._session.flush([existing])
        return existing

    async def create_or_update_bulk(self, runs: list[RunDTO]) -> None:
        if not runs:
            return

        await self._session.execute(
            insert_statement,
            [
                {
                    "created_at": run.created_at,
                    "id": run.id,
                    "job_id": run.job.id,
                    "status": RunStatus(run.status),
                    "parent_run_id": run.parent_run.id if run.parent_run else None,
                    "started_at": run.started_at,
                    "started_by_user_id": run.user.id if run.user else None,
                    "start_reason": RunStartReason(run.start_reason) if run.start_reason else None,
                    "ended_at": run.ended_at,
                    "external_id": run.external_id,
                    "attempt": run.attempt,
                    "persistent_log_url": run.persistent_log_url,
                    "running_log_url": run.running_log_url,
                    "expected_start_at": run.expected_start_at,
                    "expected_end_at": run.expected_end_at,
                }
                for run in runs
            ],
        )

    async def list_ancestor_relations(self, run_ids: Collection[UUID]):
        if not run_ids:
            return []
        stmt = select(
            ancestors_by_run_cte.c.parent_run_id,
            ancestors_by_run_cte.c.child_run_id,
        )
        result = await self._session.execute(
            stmt,
            {
                "since": extract_timestamp_from_uuid(min(run_ids)),
                "until": extract_timestamp_from_uuid(max(run_ids)),
                "run_ids": list(run_ids),
            },
        )
        return list(result.fetchall())

    async def list_descendant_relations(self, run_ids: Collection[UUID]):
        stmt = select(
            descendants_by_run_cte.c.parent_run_id,
            descendants_by_run_cte.c.child_run_id,
        )
        result = await self._session.execute(
            stmt,
            {
                "since": extract_timestamp_from_uuid(min(run_ids)),
                "until": extract_timestamp_from_uuid(max(run_ids)),
                "run_ids": list(run_ids),
            },
        )
        return list(result.fetchall())
