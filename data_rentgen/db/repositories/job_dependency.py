# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Literal

from sqlalchemy import (
    CompoundSelect,
    DateTime,
    Select,
    and_,
    any_,
    bindparam,
    literal,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased

from data_rentgen.db.models.dataset_symlink_group import DatasetSymlinkGroup
from data_rentgen.db.models.input import Input
from data_rentgen.db.models.job_dependency import JobDependency
from data_rentgen.db.models.output import Output
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import JobDependencyDTO


def _symlink_connected_cte():
    output = aliased(Output, name="connected_output")
    base_part = (
        select(
            output.dataset_id.label("original_dataset_id"),
            output.dataset_id.label("dataset_id_via_symlink"),
        )
        .where(
            output.created_at >= bindparam("since"),
            or_(
                bindparam("until", type_=DateTime(timezone=True)).is_(None),
                output.created_at <= bindparam("until"),
            ),
        )
        .distinct()
    )
    cte = base_part.cte("symlink_connected", recursive=True)
    reached_group = aliased(DatasetSymlinkGroup, name="connected_reached_group")
    next_group = aliased(DatasetSymlinkGroup, name="connected_next_group")
    recursive_part = (
        select(
            cte.c.original_dataset_id.label("original_dataset_id"),
            next_group.dataset_id.label("dataset_id_via_symlink"),
        )
        .select_from(cte)
        .join(reached_group, reached_group.dataset_id == cte.c.dataset_id_via_symlink)
        .join(next_group, next_group.fingerprint == reached_group.fingerprint)
    )
    return cte.union(recursive_part)


insert_statement = insert(JobDependency).on_conflict_do_nothing(
    index_elements=[JobDependency.from_job_id, JobDependency.to_job_id],
)


class JobDependencyRepository(Repository[JobDependency]):
    async def create_bulk(self, job_dependencies: list[JobDependencyDTO]) -> None:
        if not job_dependencies:
            return

        type_by_key: dict[tuple[int, int], str | None] = {}
        for item in job_dependencies:
            key = (item.from_job.id, item.to_job.id)
            if key not in type_by_key:
                type_by_key[key] = item.type  # type: ignore[index]
        await self._session.execute(
            insert_statement,
            [
                {"from_job_id": from_job_id, "to_job_id": to_job_id, "type": dependency_type}
                for (from_job_id, to_job_id), dependency_type in sorted(type_by_key.items())  # avoid deadlocks
            ],
        )

    async def get_dependencies(
        self,
        job_ids: list[int],
        direction: Literal["UPSTREAM", "DOWNSTREAM"],
        since: datetime | None = None,
        until: datetime | None = None,
        *,
        infer_from_lineage: bool = False,
    ) -> list[JobDependency]:
        core_query = self._get_core_hierarchy_query(include_indirect=infer_from_lineage)
        core_subquery = core_query.subquery()

        query: Select
        match direction:
            case "UPSTREAM":
                query = select(core_subquery).where(core_subquery.c.to_job_id == any_(bindparam("job_ids")))
            case "DOWNSTREAM":
                query = select(core_subquery).where(core_subquery.c.from_job_id == any_(bindparam("job_ids")))

        result = await self._session.execute(
            query,
            {"job_ids": job_ids, "since": since, "until": until},
        )
        return [
            JobDependency(from_job_id=item.from_job_id, to_job_id=item.to_job_id, type=item.type)
            for item in result.all()
        ]

    def _get_core_hierarchy_query(
        self,
        *,
        include_indirect: bool = False,
    ) -> Select | CompoundSelect:
        query: Select | CompoundSelect
        query = select(
            JobDependency.from_job_id,
            JobDependency.to_job_id,
            JobDependency.type,
        )
        if include_indirect:
            # Where clause and columns are common part for all unions
            where_clauses = [
                Input.created_at >= bindparam("since"),
                Output.created_at >= bindparam("since"),
                Output.created_at >= Input.created_at,
                Output.job_id != Input.job_id,
                or_(
                    bindparam("until", type_=DateTime(timezone=True)).is_(None),
                    and_(
                        Input.created_at <= bindparam("until"),
                        Output.created_at <= bindparam("until"),
                    ),
                ),
            ]
            inferred_columns = select(
                Output.job_id.label("from_job_id"),
                Input.job_id.label("to_job_id"),
                literal("INFERRED_FROM_LINEAGE").label("type"),
            ).distinct()

            # IO connections via same dataset
            direct_connection = inferred_columns.join(
                Input,
                Output.dataset_id == Input.dataset_id,
            ).where(*where_clauses)
            # IO connections via symlinked datasets
            connected = _symlink_connected_cte()
            via_symlinks = (
                inferred_columns.join(connected, Output.dataset_id == connected.c.original_dataset_id)
                .join(Input, connected.c.dataset_id_via_symlink == Input.dataset_id)
                .where(*where_clauses, connected.c.original_dataset_id != connected.c.dataset_id_via_symlink)
            )

            query = query.union(direct_connection, via_symlinks)

        return query
