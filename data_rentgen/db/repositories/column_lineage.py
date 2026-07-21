# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import ARRAY, ColumnElement, Integer, any_, cast, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models import ColumnLineage, DatasetColumnRelation
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import ColumnLineageDTO
from data_rentgen.utils.uuid import (
    extract_timestamp_from_uuid,
)


@dataclass(slots=True)
class ColumnLineageRow:
    source_dataset_id: int
    target_dataset_id: int
    source_column: str
    target_column: str | None
    types_combined: int
    last_used_at: datetime


class ColumnLineageRepository(Repository[ColumnLineage]):
    async def create_bulk(self, items: list[ColumnLineageDTO]):
        if not items:
            return

        insert_statement = insert(ColumnLineage)
        inserted_row = insert_statement.excluded
        statement = insert_statement.on_conflict_do_update(
            index_elements=[ColumnLineage.created_at, ColumnLineage.id],
            set_={
                # in case if job or run was changed - workaround for
                # https://github.com/OpenLineage/OpenLineage/issues/3846
                "job_id": inserted_row.job_id,
                "run_id": inserted_row.run_id,
            },
        )

        await self._session.execute(
            statement,
            [
                {
                    "id": item.generate_id(),
                    "created_at": item.created_at,
                    "operation_id": item.operation.id,
                    "run_id": item.operation.run.id,
                    "job_id": item.operation.run.job.id,  # type: ignore[arg-type]
                    "source_dataset_id": item.source_dataset.id,
                    "target_dataset_id": item.target_dataset.id,
                    "fingerprint": item.fingerprint,
                }
                for item in items
            ],
        )

    async def list_by_job_ids(
        self,
        job_ids: Collection[int],
        since: datetime,
        until: datetime | None,
        source_dataset_ids: Collection[int],
        target_dataset_ids: Collection[int],
    ):
        if not job_ids:
            return []

        where = [
            ColumnLineage.created_at >= since,
            ColumnLineage.job_id == any_(list(job_ids)),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(list(source_dataset_ids)),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(list(target_dataset_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(ColumnLineage.created_at <= until)

        return await self._get_column_lineage_with_column_relations(where)

    async def list_by_run_ids(
        self,
        run_ids: Collection[UUID],
        since: datetime,
        until: datetime | None,
        source_dataset_ids: Collection[int],
        target_dataset_ids: Collection[int],
    ):
        if not run_ids:
            return []

        where = [
            ColumnLineage.created_at >= since,
            ColumnLineage.run_id == any_(list(run_ids)),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(list(source_dataset_ids)),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(list(target_dataset_ids)),  # type: ignore[arg-type]
        ]
        if until:
            where.append(ColumnLineage.created_at <= until)
        return await self._get_column_lineage_with_column_relations(where)

    async def list_by_operation_ids(
        self,
        operation_ids: Collection[UUID],
        source_dataset_ids: Collection[int],
        target_dataset_ids: Collection[int],
    ):
        if not operation_ids:
            return []

        where = [
            # ColumnLineage created_at cannot be below operation's created_at.
            ColumnLineage.created_at >= extract_timestamp_from_uuid(min(operation_ids)),
            ColumnLineage.operation_id == any_(list(operation_ids)),  # type: ignore[arg-type]
            ColumnLineage.source_dataset_id == any_(list(source_dataset_ids)),  # type: ignore[arg-type]
            ColumnLineage.target_dataset_id == any_(list(target_dataset_ids)),  # type: ignore[arg-type]
        ]
        return await self._get_column_lineage_with_column_relations(where)

    async def list_by_dataset_pairs(
        self,
        dataset_ids_pairs: Collection[tuple[int, int]],
        since: datetime,
        until: datetime | None = None,
    ):
        if not dataset_ids_pairs:
            return []

        source_dataset_ids = [pair[0] for pair in dataset_ids_pairs]
        target_dataset_ids = [pair[1] for pair in dataset_ids_pairs]
        pairs = (
            func.unnest(
                cast(source_dataset_ids, ARRAY(Integer())),
                cast(target_dataset_ids, ARRAY(Integer())),
            )
            .table_valued("source_dataset_id", "target_dataset_id")
            .render_derived()
        )

        where = [
            ColumnLineage.created_at >= since,
            tuple_(ColumnLineage.source_dataset_id, ColumnLineage.target_dataset_id).in_(select(pairs)),
        ]
        if until:
            where.append(
                ColumnLineage.created_at <= until,
            )
        return await self._get_column_lineage_with_column_relations(where)

    async def _get_column_lineage_with_column_relations(self, where: Collection[ColumnElement]):
        query = (
            select(
                ColumnLineage.source_dataset_id,
                ColumnLineage.target_dataset_id,
                DatasetColumnRelation.source_column,
                func.nullif(DatasetColumnRelation.target_column, "").label("target_column"),
                func.bit_or(DatasetColumnRelation.type).label("types_combined"),
                func.max(ColumnLineage.created_at).label("last_used_at"),
            )
            .join(DatasetColumnRelation, ColumnLineage.fingerprint == DatasetColumnRelation.fingerprint)
            .group_by(
                ColumnLineage.source_dataset_id,
                ColumnLineage.target_dataset_id,
                DatasetColumnRelation.source_column,
                DatasetColumnRelation.target_column,
            )
        )

        query = query.where(*where)
        result = await self._session.execute(query)
        return [
            ColumnLineageRow(
                source_dataset_id=row.source_dataset_id,
                target_dataset_id=row.target_dataset_id,
                source_column=row.source_column,
                target_column=row.target_column,
                types_combined=row.types_combined,
                last_used_at=row.last_used_at,
            )
            for row in result.all()
        ]
