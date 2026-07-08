# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection
from datetime import datetime
from uuid import UUID

from sqlalchemy import Row, UnaryExpression, any_, bindparam, func, select, update
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Operation, OperationStatus, OperationType
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import OperationDTO, PaginationDTO
from data_rentgen.utils.uuid import extract_timestamp_from_uuid, get_max_uuid, get_min_uuid

insert_statement = insert(Operation).on_conflict_do_nothing()
update_statement = update(Operation)

# Do not use `tuple_(Operation.id, Operation.created_at).in_(...),
# as this is too complex filter for Postgres to make an optimal query plan.
# Primary key starts with id already, and created_at filter is used to select specific partitions
get_list_by_ids = select(Operation).where(
    Operation.created_at >= bindparam("since"),
    Operation.created_at <= bindparam("until"),
    Operation.id == any_(bindparam("operation_ids")),
)

get_stats_by_run_ids = (
    select(
        Operation.run_id.label("run_id"),
        func.count(Operation.id.distinct()).label("total_operations"),
    )
    .where(
        Operation.id >= bindparam("min_id"),
        Operation.created_at >= bindparam("since"),
        Operation.run_id == any_(bindparam("run_ids")),
    )
    .group_by(Operation.run_id)
)


class OperationRepository(Repository[Operation]):
    async def create_or_update_bulk(self, operations: list[OperationDTO]) -> None:
        if not operations:
            return
        data = [
            {
                "id": operation.id,
                "created_at": extract_timestamp_from_uuid(operation.id),
                "run_id": operation.run.id,
                "name": operation.name,
                "type": OperationType(operation.type) if operation.type else None,
                "status": OperationStatus(operation.status),
                "started_at": operation.started_at,
                "ended_at": operation.ended_at,
                "description": operation.description,
                "sql_query_id": operation.sql_query.id if operation.sql_query else None,
                "group": operation.group,
                "position": operation.position,
            }
            for operation in operations
        ]

        # this replaces all null values with defaults
        await self._session.execute(
            insert_statement,
            data,
        )

        # if value is still none, keep existing one
        await self._session.execute(
            update_statement.values(
                {
                    # in case if run was changed - workaround for
                    # https://github.com/OpenLineage/OpenLineage/issues/3846
                    "run_id": bindparam("run_id"),
                    "name": func.coalesce(bindparam("name"), Operation.name),
                    "type": func.coalesce(bindparam("type"), Operation.type),
                    "status": func.greatest(bindparam("status"), Operation.status),
                    "started_at": func.coalesce(bindparam("started_at"), Operation.started_at),
                    "ended_at": func.coalesce(bindparam("ended_at"), Operation.ended_at),
                    "description": func.coalesce(bindparam("description"), Operation.description),
                    "sql_query_id": func.coalesce(bindparam("sql_query_id"), Operation.sql_query_id),
                    "group": func.coalesce(bindparam("group"), Operation.group),
                    "position": func.coalesce(bindparam("position"), Operation.position),
                },
            ),
            data,
        )

    async def paginate(
        self,
        page: int,
        page_size: int,
        operation_ids: Collection[UUID],
        since: datetime | None,
        until: datetime | None,
        run_ids: Collection[UUID],
    ) -> PaginationDTO[Operation]:
        # do not use `tuple_(Operation.id, Operation.created_at).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        where = []

        # created_at and id are always correlated,
        # and primary key starts with id, so we need to apply filter on both
        # to get the most optimal query plan
        if operation_ids:
            min_operation_created_at = extract_timestamp_from_uuid(min(operation_ids))
            max_operation_created_at = extract_timestamp_from_uuid(max(operation_ids))
            # narrow created_at range
            min_created_at = max(filter(None, [since, min_operation_created_at]))
            max_created_at = min(filter(None, [until, max_operation_created_at]))
            where = [
                Operation.created_at >= min_created_at,
                Operation.created_at <= max_created_at,
                Operation.id == any_(list(operation_ids)),  # type: ignore[arg-type]
            ]

        elif run_ids:
            run_created_at = extract_timestamp_from_uuid(min(run_ids))
            # narrow created_at range
            min_created_at = max(filter(None, [since, run_created_at]))
            where = [
                Operation.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
                Operation.created_at >= min_created_at,
                Operation.id >= get_min_uuid(min_created_at),
            ]

        elif since:
            where = [
                Operation.created_at >= since,
                Operation.id >= get_min_uuid(since),
            ]

        if until and not operation_ids:
            where += [
                Operation.created_at <= until,
                Operation.id <= get_max_uuid(until),
            ]

        query = select(Operation).where(*where)
        order_by: list[UnaryExpression] = [Operation.id.desc(), Operation.created_at.desc()]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            page=page,
            page_size=page_size,
        )

    async def list_by_ids(self, operation_ids: Collection[UUID]) -> list[Operation]:
        if not operation_ids:
            return []

        result = await self._session.scalars(
            get_list_by_ids,
            {
                "since": extract_timestamp_from_uuid(min(operation_ids)),
                "until": extract_timestamp_from_uuid(max(operation_ids)),
                "operation_ids": list(operation_ids),
            },
        )
        return list(result.all())

    async def get_stats_by_run_ids(self, run_ids: Collection[UUID]) -> dict[UUID, Row]:
        if not run_ids:
            return {}

        # All operations are created after run
        since = extract_timestamp_from_uuid(min(run_ids))
        query_result = await self._session.execute(
            get_stats_by_run_ids,
            {
                "since": since,
                "min_id": get_min_uuid(since),
                "run_ids": list(run_ids),
            },
        )
        return {row.run_id: row for row in query_result.all()}
