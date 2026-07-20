# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from sqlalchemy import ColumnElement, Row, Select, Subquery, any_, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import Output, OutputType, Schema
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import OutputDTO
from data_rentgen.utils.uuid import (
    extract_timestamp_from_uuid,
)

insert_statement = insert(Output)
inserted_row = insert_statement.excluded
insert_statement = insert_statement.on_conflict_do_update(
    index_elements=[Output.created_at, Output.id],
    set_={
        # in case if job or run was changed - workaround for
        # https://github.com/OpenLineage/OpenLineage/issues/3846
        "job_id": inserted_row.job_id,
        "run_id": inserted_row.run_id,
        "type": inserted_row.type.op("|")(Output.type),
        "num_bytes": func.greatest(inserted_row.num_bytes, Output.num_bytes),
        "num_rows": func.greatest(inserted_row.num_rows, Output.num_rows),
        "num_files": func.greatest(inserted_row.num_files, Output.num_files),
    },
)


@dataclass
class OutputRow:
    created_at: datetime
    operation_id: UUID
    run_id: UUID
    job_id: int
    dataset_id: int
    num_bytes: int | None
    num_rows: int | None
    num_files: int | None
    types_combined: int | None = None
    schema_id: int | None = None
    schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None = None
    schema: Schema | None = None


class OutputRepository(Repository[Output]):
    async def create_or_update_bulk(self, outputs: list[OutputDTO]) -> None:
        if not outputs:
            return

        await self._session.execute(
            insert_statement,
            [
                {
                    "id": item.generate_id(),
                    "created_at": item.created_at,
                    "type": OutputType(item.type),
                    "operation_id": item.operation.id,
                    "run_id": item.operation.run.id,
                    "job_id": item.operation.run.job.id,  # type: ignore[arg-type]
                    "dataset_id": item.dataset.id,  # type: ignore[arg-type]
                    "schema_id": item.schema.id if item.schema else None,
                    "num_bytes": item.num_bytes,
                    "num_rows": item.num_rows,
                    "num_files": item.num_files,
                }
                for item in outputs
            ],
        )

    async def list_by_operation_ids(
        self,
        operation_ids: Collection[UUID],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[OutputRow]:
        if not operation_ids:
            return []

        # Output created_at cannot be before operation's created_at
        # do not use `tuple_(Output.created_at, Output.operation_id).in_(...),
        # as this is too complex filter for Postgres to make an optimal query plan
        where = [
            Output.created_at >= extract_timestamp_from_uuid(min(operation_ids)),
            Output.operation_id == any_(list(operation_ids)),  # type: ignore[arg-type]
        ]
        return await self._get_outputs(where, granularity)

    async def list_by_run_ids(
        self,
        run_ids: Collection[UUID],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[OutputRow]:
        if not run_ids:
            return []

        min_run_created_at = extract_timestamp_from_uuid(min(run_ids))
        min_created_at = max(min_run_created_at, since.astimezone(timezone.utc))
        where = [
            Output.created_at >= min_created_at,
            Output.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    async def list_by_job_ids(
        self,
        job_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[OutputRow]:
        if not job_ids:
            return []

        where = [
            Output.created_at >= since,
            Output.job_id == any_(list(job_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    async def list_by_dataset_ids(
        self,
        dataset_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[OutputRow]:
        if not dataset_ids:
            return []

        where = [
            Output.created_at >= since,
            Output.dataset_id == any_(list(dataset_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(Output.created_at <= until)

        return await self._get_outputs(where, granularity)

    def _get_base_query(
        self,
        where: Collection[ColumnElement],
    ) -> Select:
        # For operation.type=STREAMING or long-running operation.type=BATCH,
        # there can be multiple outputs for same operation+dataset+schema,
        # so we need to get latest output for each operation before applying sum on it
        partition_columns = [Output.dataset_id, Output.job_id, Output.run_id, Output.operation_id]

        # Avoid sorting multiple times by different keys for performance reason,
        # instead reuse the same window expression
        def window(expr):
            return expr.over(
                partition_by=partition_columns,
                order_by=[Output.created_at, Output.schema_id],
                range_=(None, None),  # important
            )

        return (
            select(
                *partition_columns,
                window(func.last_value(Output.created_at)).label("created_at"),
                window(func.last_value(Output.num_bytes)).label("num_bytes"),
                window(func.last_value(Output.num_rows)).label("num_rows"),
                window(func.last_value(Output.num_files)).label("num_files"),
                window(func.bit_or(Output.type)).label("type"),
                window(func.first_value(Output.schema_id)).label("oldest_schema_id"),
                window(func.last_value(Output.schema_id)).label("newest_schema_id"),
            )
            .distinct(*partition_columns)
            .where(*where)
        )

    def _query_for_granularity(
        self,
        base_query: Subquery,
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ):
        if granularity == "OPERATION":
            query = select(
                func.max(base_query.c.created_at).label("max_created_at"),
                base_query.c.operation_id,
                base_query.c.run_id,
                base_query.c.job_id,
                base_query.c.dataset_id,
                func.sum(base_query.c.num_bytes).label("sum_num_bytes"),
                func.sum(base_query.c.num_rows).label("sum_num_rows"),
                func.sum(base_query.c.num_files).label("sum_num_files"),
                func.bit_or(base_query.c.type).label("types_combined"),
                func.min(base_query.c.oldest_schema_id).label("min_schema_id"),
                func.max(base_query.c.newest_schema_id).label("max_schema_id"),
            ).group_by(
                base_query.c.dataset_id,
                base_query.c.job_id,
                base_query.c.run_id,
                base_query.c.operation_id,
            )
        elif granularity == "RUN":
            query = select(
                func.max(base_query.c.created_at).label("max_created_at"),
                literal_column("NULL").label("operation_id"),
                base_query.c.run_id,
                base_query.c.job_id,
                base_query.c.dataset_id,
                func.sum(base_query.c.num_bytes).label("sum_num_bytes"),
                func.sum(base_query.c.num_rows).label("sum_num_rows"),
                func.sum(base_query.c.num_files).label("sum_num_files"),
                func.bit_or(base_query.c.type).label("types_combined"),
                func.min(base_query.c.oldest_schema_id).label("min_schema_id"),
                func.max(base_query.c.newest_schema_id).label("max_schema_id"),
            ).group_by(
                base_query.c.dataset_id,
                base_query.c.job_id,
                base_query.c.run_id,
            )
        else:
            query = select(
                func.max(base_query.c.created_at).label("max_created_at"),
                literal_column("NULL").label("operation_id"),
                literal_column("NULL").label("run_id"),
                base_query.c.job_id,
                base_query.c.dataset_id,
                func.sum(base_query.c.num_bytes).label("sum_num_bytes"),
                func.sum(base_query.c.num_rows).label("sum_num_rows"),
                func.sum(base_query.c.num_files).label("sum_num_files"),
                func.bit_or(base_query.c.type).label("types_combined"),
                func.min(base_query.c.oldest_schema_id).label("min_schema_id"),
                func.max(base_query.c.newest_schema_id).label("max_schema_id"),
            ).group_by(
                base_query.c.dataset_id,
                base_query.c.job_id,
            )
        return query

    async def _get_outputs(
        self,
        where: Collection[ColumnElement],
        granularity: Literal["JOB", "RUN", "OPERATION"],
    ) -> list[OutputRow]:
        base_query = self._get_base_query(where).subquery()
        if granularity == "OPERATION":
            query = self._query_for_granularity(base_query, "OPERATION").union_all(
                self._query_for_granularity(base_query, "RUN"),
                self._query_for_granularity(base_query, "JOB"),
            )
        elif granularity == "RUN":
            query = self._query_for_granularity(base_query, "RUN").union_all(
                self._query_for_granularity(base_query, "JOB")
            )
        else:
            query = self._query_for_granularity(base_query, "JOB")

        query_result = await self._session.execute(query)
        results = []
        for row in query_result.all():
            schema_relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None
            if row.max_schema_id:
                schema_relevance_type = "EXACT_MATCH" if row.min_schema_id == row.max_schema_id else "LATEST_KNOWN"
            else:
                schema_relevance_type = None
            results.append(
                OutputRow(
                    created_at=row.max_created_at,
                    operation_id=row.operation_id,
                    run_id=row.run_id,
                    job_id=row.job_id,
                    dataset_id=row.dataset_id,
                    num_bytes=row.sum_num_bytes,
                    num_rows=row.sum_num_rows,
                    num_files=row.sum_num_files,
                    types_combined=row.types_combined,
                    schema_id=row.max_schema_id,
                    schema_relevance_type=schema_relevance_type,
                ),
            )
        return results

    async def get_stats_by_operation_ids(self, operation_ids: Collection[UUID]) -> dict[UUID, Row]:
        if not operation_ids:
            return {}

        where = [
            # Output created_at cannot be before operation's created_at
            Output.created_at >= extract_timestamp_from_uuid(min(operation_ids)),
            Output.operation_id == any_(list(operation_ids)),  # type: ignore[arg-type]
        ]

        base_query = self._get_base_query(where).subquery()
        query = select(
            base_query.c.operation_id.label("operation_id"),
            func.count(base_query.c.dataset_id.distinct()).label("total_datasets"),
            func.sum(base_query.c.num_bytes).label("total_bytes"),
            func.sum(base_query.c.num_rows).label("total_rows"),
            func.sum(base_query.c.num_files).label("total_files"),
        ).group_by(base_query.c.operation_id)

        query_result = await self._session.execute(query)
        return {row.operation_id: row for row in query_result.all()}

    async def get_stats_by_run_ids(self, run_ids: Collection[UUID]) -> dict[UUID, Row]:
        if not run_ids:
            return {}

        # unlike list_by_run_ids, we need to get all statistics for specific runs, regardless of time range
        where = [
            Output.created_at >= extract_timestamp_from_uuid(min(run_ids)),
            Output.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
        ]

        base_query = self._get_base_query(where).subquery()
        query = select(
            base_query.c.run_id.label("run_id"),
            func.count(base_query.c.dataset_id.distinct()).label("total_datasets"),
            func.sum(base_query.c.num_bytes).label("total_bytes"),
            func.sum(base_query.c.num_rows).label("total_rows"),
            func.sum(base_query.c.num_files).label("total_files"),
        ).group_by(base_query.c.run_id)

        query_result = await self._session.execute(query)
        return {row.run_id: row for row in query_result.all()}
