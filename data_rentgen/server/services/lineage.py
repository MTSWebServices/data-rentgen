# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Literal, NamedTuple
from uuid import UUID

from fastapi import Depends

from data_rentgen.db.models import (
    Dataset,
    DatasetSymlinkGroup,
    DatasetSymlinkType,
    Job,
    Operation,
    Run,
)
from data_rentgen.db.repositories.column_lineage import ColumnLineageRow
from data_rentgen.db.repositories.input import InputRow
from data_rentgen.db.repositories.io_dataset_relation import IODatasetRelationRow
from data_rentgen.db.repositories.output import OutputRow
from data_rentgen.server.schemas.v1.lineage import LineageDirectionV1
from data_rentgen.services.uow import UnitOfWork

logger = logging.getLogger(__name__)


class SymlinkPair(NamedTuple):
    from_dataset_id: int
    to_dataset_id: int
    type: DatasetSymlinkType


def _reconstruct_symlink_pairs(symlink_groups: list[DatasetSymlinkGroup]) -> list[SymlinkPair]:
    members_by_fingerprint: dict[UUID, list[tuple[int, DatasetSymlinkType]]] = defaultdict(list)
    for row in symlink_groups:
        members_by_fingerprint[row.fingerprint].append((row.dataset_id, row.type))

    pairs: dict[tuple[int, int], SymlinkPair] = {}
    for members in members_by_fingerprint.values():
        for from_dataset_id, _ in members:
            for to_dataset_id, to_type in members:
                if from_dataset_id != to_dataset_id:
                    pairs[(from_dataset_id, to_dataset_id)] = SymlinkPair(
                        from_dataset_id=from_dataset_id,
                        to_dataset_id=to_dataset_id,
                        type=to_type,
                    )
    return list(pairs.values())


@dataclass
class LineageServiceIntermediateResult:
    job_ids: set[int] = field(default_factory=set)
    run_ids: set[UUID] = field(default_factory=set)
    operation_ids: set[UUID] = field(default_factory=set)
    dataset_ids: set[int] = field(default_factory=set)
    symlink_groups: list[DatasetSymlinkGroup] = field(default_factory=list)
    inputs: list[InputRow] = field(default_factory=list)
    outputs: list[OutputRow] = field(default_factory=list)

    def merge(self, other: "LineageServiceIntermediateResult") -> "LineageServiceIntermediateResult":
        self.job_ids |= other.job_ids
        self.run_ids |= other.run_ids
        self.operation_ids |= other.operation_ids
        self.dataset_ids |= other.dataset_ids
        self.symlink_groups.extend(other.symlink_groups)
        self.inputs.extend(other.inputs)
        self.outputs.extend(other.outputs)
        return self


@dataclass
class LineageServiceResult:
    jobs: dict[int, Job] = field(default_factory=dict)
    runs: dict[UUID, Run] = field(default_factory=dict)
    operations: dict[UUID, Operation] = field(default_factory=dict)
    datasets: dict[int, Dataset] = field(default_factory=dict)
    dataset_symlinks: dict[tuple[int, int], SymlinkPair] = field(default_factory=dict)
    inputs: dict[tuple[int, int, UUID | None, UUID | None], InputRow] = field(default_factory=dict)
    outputs: dict[tuple[int, int, UUID | None, UUID | None, int | None], OutputRow] = field(default_factory=dict)
    column_lineage: dict[tuple[int, int], list[ColumnLineageRow]] = field(default_factory=dict)
    io_dataset_relations: dict[tuple[int, int], IODatasetRelationRow] = field(default_factory=dict)
    run_ancestor_relations: set[tuple[UUID, UUID]] = field(default_factory=set)
    job_ancestor_relations: set[tuple[int, int]] = field(default_factory=set)


class LineageService:
    def __init__(self, uow: Annotated[UnitOfWork, Depends()]):
        self._uow = uow

    async def get_lineage_by_job(
        self,
        *,
        job_id: int,
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,
    ) -> LineageServiceResult:
        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            upstream = await self._get_lineage_by_jobs_recursive(
                job_ids={job_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.UPSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = upstream

        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            downstream = await self._get_lineage_by_jobs_recursive(
                job_ids={job_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.DOWNSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = downstream

        if direction == LineageDirectionV1.BOTH:
            seen = upstream.merge(downstream)

        operations = await self._uow.operation.list_by_ids(seen.operation_ids)

        seen.run_ids |= {operation.run_id for operation in operations}
        run_ancestors = await self._uow.run.list_ancestor_relations(seen.run_ids)
        runs = await self._uow.run.list_by_ids(seen.run_ids | {p_id for p_id, _ in run_ancestors})

        seen.job_ids |= {run.job_id for run in runs}
        job_ancestors = await self._uow.job.list_ancestor_relations(seen.job_ids)
        jobs = await self._uow.job.list_by_ids(seen.job_ids | {p_id for p_id, _ in job_ancestors})

        datasets = await self._uow.dataset.list_by_ids(seen.dataset_ids)
        await self._fill_input_output_schemas(seen)

        if include_column_lineage:
            column_lineage = await self._uow.column_lineage.list_by_job_ids(
                job_ids=seen.job_ids,
                since=since,
                until=until,
                source_dataset_ids=[input_.dataset_id for input_ in seen.inputs],
                target_dataset_ids=[output.dataset_id for output in seen.outputs],
            )
        else:
            column_lineage = []

        result = LineageServiceResult(
            jobs={job.id: job for job in jobs},
            runs={run.id: run for run in runs},
            operations={operation.id: operation for operation in operations},
            datasets={dataset.id: dataset for dataset in datasets},
            dataset_symlinks={
                (pair.from_dataset_id, pair.to_dataset_id): pair
                for pair in _reconstruct_symlink_pairs(seen.symlink_groups)
            },
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in seen.inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in seen.outputs
            },
            column_lineage=self._build_column_lineage(column_lineage),
            run_ancestor_relations=run_ancestors,
            job_ancestor_relations=job_ancestors,
        )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Total] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def _get_lineage_by_jobs_recursive(
        self,
        job_ids: set[int],
        seen: LineageServiceIntermediateResult,
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        level: int = 1,
    ) -> LineageServiceIntermediateResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by jobs %r, with direction %s, since %s, until %s",
                level,
                sorted(job_ids),
                direction,
                since,
                until,
            )

        # include all child jobs
        child_jobs = await self._uow.job.list_descendant_relations(job_ids)
        if child_jobs and logger.isEnabledFor(logging.INFO):
            logger.info("[Level %d] Including %d child jobs", level, len(child_jobs))
        job_ids |= {c_id for _, c_id in child_jobs}

        inputs: list[InputRow] = []
        if direction == LineageDirectionV1.UPSTREAM:
            inputs = await self._uow.input.list_by_job_ids(
                job_ids=job_ids - seen.job_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        outputs: list[OutputRow] = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_job_ids(
                job_ids=job_ids - seen.job_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        seen.job_ids |= job_ids
        seen.run_ids |= {input_.run_id for input_ in inputs if input_.run_id is not None} | {
            output.run_id for output in outputs if output.run_id is not None
        }
        seen.operation_ids |= {input_.operation_id for input_ in inputs if input_.operation_id is not None} | {
            output.operation_id for output in outputs if output.operation_id is not None
        }
        seen.inputs += inputs
        seen.outputs += outputs

        dataset_ids = (
            {input_.dataset_id for input_ in seen.inputs} | {output.dataset_id for output in seen.outputs}
        ) - seen.dataset_ids

        symlink_groups = await self._uow.dataset_symlink.get_symlink_groups(dataset_ids)
        dataset_ids |= {symlink.dataset_id for symlink in symlink_groups} - seen.dataset_ids
        seen.symlink_groups += symlink_groups

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d datasets by %d jobs",
                level,
                len(dataset_ids),
                len(job_ids),
            )

        if not dataset_ids:
            return seen

        if depth == 1:
            seen.dataset_ids |= dataset_ids
            return seen

        return await self._get_lineage_by_datasets_recursive(
            dataset_ids=dataset_ids,
            seen=seen,
            direction=direction,
            granularity=granularity,
            since=since,
            until=until,
            depth=depth - 1,
            level=level + 1,
        )

    async def get_lineage_by_run(
        self,
        *,
        run_id: UUID,
        direction: LineageDirectionV1,
        granularity: Literal["RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,
    ) -> LineageServiceResult:
        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            upstream = await self._get_lineage_by_runs_recursive(
                run_ids={run_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.UPSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = upstream

        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            downstream = await self._get_lineage_by_runs_recursive(
                run_ids={run_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.DOWNSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = downstream

        if direction == LineageDirectionV1.BOTH:
            seen = upstream.merge(downstream)

        operations = await self._uow.operation.list_by_ids(seen.operation_ids)

        seen.run_ids |= {operation.run_id for operation in operations}
        run_ancestors = await self._uow.run.list_ancestor_relations(seen.run_ids)
        runs = await self._uow.run.list_by_ids(seen.run_ids | {p_id for p_id, _ in run_ancestors})

        seen.job_ids |= {run.job_id for run in runs}
        job_ancestors = await self._uow.job.list_ancestor_relations(seen.job_ids)
        jobs = await self._uow.job.list_by_ids(seen.job_ids | {p_id for p_id, _ in job_ancestors})

        datasets = await self._uow.dataset.list_by_ids(seen.dataset_ids)
        await self._fill_input_output_schemas(seen)

        if include_column_lineage:
            column_lineage = await self._uow.column_lineage.list_by_run_ids(
                run_ids=seen.run_ids,
                since=since,
                until=until,
                source_dataset_ids={input_.dataset_id for input_ in seen.inputs},
                target_dataset_ids={output.dataset_id for output in seen.outputs},
            )
        else:
            column_lineage = []

        result = LineageServiceResult(
            jobs={job.id: job for job in jobs},
            runs={run.id: run for run in runs},
            operations={operation.id: operation for operation in operations},
            datasets={dataset.id: dataset for dataset in datasets},
            dataset_symlinks={
                (pair.from_dataset_id, pair.to_dataset_id): pair
                for pair in _reconstruct_symlink_pairs(seen.symlink_groups)
            },
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in seen.inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in seen.outputs
            },
            column_lineage=self._build_column_lineage(column_lineage),
            run_ancestor_relations=run_ancestors,
            job_ancestor_relations=job_ancestors,
        )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Total] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def _get_lineage_by_runs_recursive(
        self,
        run_ids: set[UUID],
        seen: LineageServiceIntermediateResult,
        direction: LineageDirectionV1,
        granularity: Literal["RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        level: int = 1,
    ) -> LineageServiceIntermediateResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by runs %r, with direction %s, since %s, until %s",
                level,
                sorted(run_ids),
                direction,
                since,
                until,
            )

        # include all child runs
        child_runs = await self._uow.run.list_descendant_relations(run_ids)
        if child_runs and logger.isEnabledFor(logging.INFO):
            logger.info("[Level %d] Including %d child runs", level, len(child_runs))
        run_ids |= {c_id for _, c_id in child_runs}

        inputs: list[InputRow] = []
        if direction == LineageDirectionV1.UPSTREAM:
            inputs = await self._uow.input.list_by_run_ids(
                run_ids=run_ids - seen.run_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        outputs: list[OutputRow] = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_run_ids(
                run_ids=run_ids - seen.run_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        dataset_ids = (
            {input_.dataset_id for input_ in inputs} | {output.dataset_id for output in outputs}
        ) - seen.dataset_ids

        symlink_groups = await self._uow.dataset_symlink.get_symlink_groups(dataset_ids)
        dataset_ids |= {symlink.dataset_id for symlink in symlink_groups} - seen.dataset_ids
        seen.symlink_groups += symlink_groups

        seen.run_ids |= run_ids
        seen.operation_ids |= {input_.operation_id for input_ in inputs if input_.operation_id is not None} | {
            output.operation_id for output in outputs if output.operation_id is not None
        }
        seen.job_ids |= {input_.job_id for input_ in inputs} | {output.job_id for output in outputs}
        seen.inputs += inputs
        seen.outputs += outputs

        logger.info(
            "[Level %d] Found %d datasets by %d runs",
            level,
            len(dataset_ids),
            len(run_ids),
        )

        if not dataset_ids:
            return seen

        if depth == 1:
            seen.dataset_ids |= dataset_ids
            return seen

        return await self._get_lineage_by_datasets_recursive(
            dataset_ids=dataset_ids,
            seen=seen,
            direction=direction,
            granularity=granularity,
            since=since,
            until=until,
            depth=depth - 1,
            level=level + 1,
        )

    async def get_lineage_by_operation(
        self,
        *,
        operation_id: UUID,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,
    ) -> LineageServiceResult:
        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            upstream = await self._get_lineage_by_operations_recursive(
                operation_ids={operation_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.UPSTREAM,
                since=since,
                until=until,
                depth=depth,
            )
            seen = upstream

        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            downstream = await self._get_lineage_by_operations_recursive(
                operation_ids={operation_id},
                seen=LineageServiceIntermediateResult(),
                direction=LineageDirectionV1.DOWNSTREAM,
                since=since,
                until=until,
                depth=depth,
            )
            seen = downstream

        if direction == LineageDirectionV1.BOTH:
            seen = upstream.merge(downstream)

        operations = await self._uow.operation.list_by_ids(seen.operation_ids)

        seen.run_ids |= {operation.run_id for operation in operations}
        run_ancestors = await self._uow.run.list_ancestor_relations(seen.run_ids)
        runs = await self._uow.run.list_by_ids(seen.run_ids | {p_id for p_id, _ in run_ancestors})

        seen.job_ids |= {run.job_id for run in runs}
        job_ancestors = await self._uow.job.list_ancestor_relations(seen.job_ids)
        jobs = await self._uow.job.list_by_ids(seen.job_ids | {p_id for p_id, _ in job_ancestors})

        datasets = await self._uow.dataset.list_by_ids(seen.dataset_ids)
        await self._fill_input_output_schemas(seen)

        if include_column_lineage:
            column_lineage = await self._uow.column_lineage.list_by_operation_ids(
                operation_ids=seen.operation_ids,
                source_dataset_ids={input_.dataset_id for input_ in seen.inputs},
                target_dataset_ids={output.dataset_id for output in seen.outputs},
            )
        else:
            column_lineage = []

        result = LineageServiceResult(
            jobs={job.id: job for job in jobs},
            runs={run.id: run for run in runs},
            operations={operation.id: operation for operation in operations},
            datasets={dataset.id: dataset for dataset in datasets},
            dataset_symlinks={
                (pair.from_dataset_id, pair.to_dataset_id): pair
                for pair in _reconstruct_symlink_pairs(seen.symlink_groups)
            },
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in seen.inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in seen.outputs
            },
            column_lineage=self._build_column_lineage(column_lineage),
            run_ancestor_relations=run_ancestors,
            job_ancestor_relations=job_ancestors,
        )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Total] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def _get_lineage_by_operations_recursive(
        self,
        operation_ids: set[UUID],
        seen: LineageServiceIntermediateResult,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        level: int = 1,
    ) -> LineageServiceIntermediateResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by operations %r, with direction %s, since %s, until %s",
                level,
                sorted(operation_ids),
                direction,
                since,
                until,
            )

        inputs: list[InputRow] = []
        if direction == LineageDirectionV1.UPSTREAM:
            inputs = await self._uow.input.list_by_operation_ids(
                operation_ids=operation_ids - seen.run_ids,
                granularity="OPERATION",
            )

        outputs: list[OutputRow] = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            outputs = await self._uow.output.list_by_operation_ids(
                operation_ids=operation_ids - seen.run_ids,
                granularity="OPERATION",
            )

        dataset_ids = (
            {input_.dataset_id for input_ in inputs} | {output.dataset_id for output in outputs}
        ) - seen.dataset_ids

        symlink_groups = await self._uow.dataset_symlink.get_symlink_groups(dataset_ids)
        dataset_ids |= {symlink.dataset_id for symlink in symlink_groups} - seen.dataset_ids
        seen.symlink_groups += symlink_groups

        seen.operation_ids |= operation_ids
        seen.run_ids |= {input_.run_id for input_ in inputs if input_.run_id is not None} | {
            output.run_id for output in outputs if output.run_id is not None
        }
        seen.job_ids |= {input_.job_id for input_ in inputs} | {output.job_id for output in outputs}
        seen.inputs += inputs
        seen.outputs += outputs

        logger.info(
            "[Level %d] Found %d datasets by %d operations",
            level,
            len(dataset_ids),
            len(operation_ids),
        )

        if not dataset_ids:
            return seen

        if depth == 1:
            seen.dataset_ids |= dataset_ids
            return seen

        return await self._get_lineage_by_datasets_recursive(
            dataset_ids=dataset_ids,
            seen=seen,
            direction=direction,
            granularity="OPERATION",
            since=since,
            until=until,
            depth=depth - 1,
            level=level + 1,
        )

    async def get_lineage_by_dataset(
        self,
        *,
        dataset_id: int,
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool = False,
    ) -> LineageServiceResult:
        # include dataset symlinks
        symlink_groups = await self._uow.dataset_symlink.get_symlink_groups([dataset_id])
        dataset_ids = {dataset_id} | {symlink.dataset_id for symlink in symlink_groups}

        if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
            upstream = await self._get_lineage_by_datasets_recursive(
                dataset_ids=dataset_ids,
                seen=LineageServiceIntermediateResult(symlink_groups=symlink_groups),
                direction=LineageDirectionV1.UPSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = upstream

        if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
            downstream = await self._get_lineage_by_datasets_recursive(
                dataset_ids=dataset_ids,
                seen=LineageServiceIntermediateResult(symlink_groups=symlink_groups),
                direction=LineageDirectionV1.DOWNSTREAM,
                granularity=granularity,
                since=since,
                until=until,
                depth=depth,
            )
            seen = downstream

        if direction == LineageDirectionV1.BOTH:
            seen = upstream.merge(downstream)

        operations = await self._uow.operation.list_by_ids(seen.operation_ids)

        seen.run_ids |= {operation.run_id for operation in operations}
        run_ancestors = await self._uow.run.list_ancestor_relations(seen.run_ids)
        runs = await self._uow.run.list_by_ids(seen.run_ids | {p_id for p_id, _ in run_ancestors})

        seen.job_ids |= {run.job_id for run in runs}
        job_ancestors = await self._uow.job.list_ancestor_relations(seen.job_ids)
        jobs = await self._uow.job.list_by_ids(seen.job_ids | {p_id for p_id, _ in job_ancestors})

        datasets = await self._uow.dataset.list_by_ids(seen.dataset_ids)
        await self._fill_input_output_schemas(seen)

        if include_column_lineage:
            if seen.operation_ids:
                column_lineage = await self._uow.column_lineage.list_by_operation_ids(
                    operation_ids=seen.operation_ids,
                    source_dataset_ids={input_.dataset_id for input_ in seen.inputs},
                    target_dataset_ids={output.dataset_id for output in seen.outputs},
                )
            elif seen.run_ids:
                column_lineage = await self._uow.column_lineage.list_by_run_ids(
                    run_ids=seen.run_ids,
                    source_dataset_ids={input_.dataset_id for input_ in seen.inputs},
                    target_dataset_ids={output.dataset_id for output in seen.outputs},
                    since=since,
                    until=until,
                )
            else:
                column_lineage = await self._uow.column_lineage.list_by_job_ids(
                    job_ids=seen.job_ids,
                    source_dataset_ids={input_.dataset_id for input_ in seen.inputs},
                    target_dataset_ids={output.dataset_id for output in seen.outputs},
                    since=since,
                    until=until,
                )
        else:
            column_lineage = []

        result = LineageServiceResult(
            jobs={job.id: job for job in jobs},
            runs={run.id: run for run in runs},
            operations={operation.id: operation for operation in operations},
            datasets={dataset.id: dataset for dataset in datasets},
            dataset_symlinks={
                (pair.from_dataset_id, pair.to_dataset_id): pair
                for pair in _reconstruct_symlink_pairs(seen.symlink_groups)
            },
            inputs={
                (input_.dataset_id, input_.job_id, input_.run_id, input_.operation_id): input_ for input_ in seen.inputs
            },
            outputs={
                (output.dataset_id, output.job_id, output.run_id, output.operation_id, output.types_combined): output
                for output in seen.outputs
            },
            column_lineage=self._build_column_lineage(column_lineage),
            run_ancestor_relations=run_ancestors,
            job_ancestor_relations=job_ancestors,
        )

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Total] Found %d jobs, %d runs, %d operations, %d datasets, %d dataset symlinks, "
                "%d inputs, %d outputs, %d column lineage",
                len(result.jobs),
                len(result.runs),
                len(result.operations),
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.inputs),
                len(result.outputs),
                len(result.column_lineage),
            )
        return result

    async def _get_lineage_by_datasets_recursive(
        self,
        dataset_ids: set[int],
        seen: LineageServiceIntermediateResult,
        direction: LineageDirectionV1,
        granularity: Literal["JOB", "RUN", "OPERATION"],
        since: datetime,
        until: datetime | None,
        depth: int,
        level: int = 1,
    ) -> LineageServiceIntermediateResult:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Get lineage by datasets %r, with direction %s, since %s, until %s",
                level,
                sorted(dataset_ids),
                direction,
                since,
                until,
            )

        outputs: list[OutputRow] = []
        if direction == LineageDirectionV1.UPSTREAM:
            # JOB|RUN|OPERATION -> DATASET is UPSTREAM from dataset perspective
            outputs += await self._uow.output.list_by_dataset_ids(
                dataset_ids=dataset_ids - seen.dataset_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        inputs: list[InputRow] = []
        if direction == LineageDirectionV1.DOWNSTREAM:
            # DATASET -> JOB|RUN|OPERATION is DOWNSTREAM from dataset perspective
            inputs += await self._uow.input.list_by_dataset_ids(
                dataset_ids=dataset_ids - seen.dataset_ids,
                since=since,
                until=until,
                granularity=granularity,
            )

        seen.dataset_ids |= dataset_ids
        seen.inputs += inputs
        seen.outputs += outputs

        job_ids = ({output.job_id for output in outputs} | {input_.job_id for input_ in inputs}) - seen.job_ids

        run_ids = (
            {output.run_id for output in outputs if output.run_id is not None}
            | {input_.run_id for input_ in inputs if input_.run_id is not None}
        ) - seen.run_ids

        operation_ids = (
            {output.operation_id for output in outputs if output.operation_id is not None}
            | {input_.operation_id for input_ in inputs if input_.operation_id is not None}
        ) - seen.operation_ids

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Level %d] Found %d jobs, %d runs, %d operations",
                level,
                len(job_ids),
                len(run_ids),
                len(operation_ids),
            )

        if not operation_ids and not run_ids and not job_ids:
            return seen

        if depth == 1:
            seen.job_ids |= job_ids
            seen.run_ids |= run_ids
            seen.operation_ids |= operation_ids
            return seen

        match granularity:
            case "OPERATION":
                return await self._get_lineage_by_operations_recursive(
                    operation_ids=operation_ids,
                    seen=seen,
                    direction=direction,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                )
            case "RUN":
                return await self._get_lineage_by_runs_recursive(
                    run_ids=run_ids,
                    seen=seen,
                    direction=direction,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                )
            case "JOB":
                return await self._get_lineage_by_jobs_recursive(
                    job_ids=job_ids,
                    seen=seen,
                    direction=direction,
                    granularity=granularity,
                    since=since,
                    until=until,
                    depth=depth - 1,
                    level=level + 1,
                )

    async def get_lineage_by_dataset_with_dataset_granularity(  # noqa: C901, PLR0912, PLR0915
        self,
        *,
        dataset_id: int,
        direction: LineageDirectionV1,
        since: datetime,
        until: datetime | None,
        depth: int,
        include_column_lineage: bool,
    ) -> LineageServiceResult:
        # include symlinks for starting datasets
        symlink_groups = await self._uow.dataset_symlink.get_symlink_groups([dataset_id])
        all_dataset_ids = {dataset_id} | {symlink.dataset_id for symlink in symlink_groups}
        next_level_downstream_dataset_ids = all_dataset_ids.copy()
        next_level_upstream_dataset_ids = all_dataset_ids.copy()

        level = 0
        result = LineageServiceResult()
        while depth:
            if not next_level_downstream_dataset_ids and not next_level_upstream_dataset_ids:
                break
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "[Level %d] Get lineage by datasets %r, with direction %s, since %s, until %s",
                    level,
                    sorted(next_level_downstream_dataset_ids | next_level_upstream_dataset_ids),
                    direction,
                    since,
                    until,
                )

            relations_by_id = {}
            if direction in {LineageDirectionV1.DOWNSTREAM, LineageDirectionV1.BOTH}:
                downstream_relations = await self._uow.io_dataset_relation.get_relations(
                    next_level_downstream_dataset_ids,
                    since=since,
                    until=until,
                    direction="DOWNSTREAM",
                )
                relations_by_id.update(
                    {(relation.in_dataset_id, relation.out_dataset_id): relation for relation in downstream_relations},
                )
                next_level_downstream_dataset_ids = {
                    relation.out_dataset_id for relation in downstream_relations
                } - all_dataset_ids

                extra_symlink_groups = await self._uow.dataset_symlink.get_symlink_groups(
                    next_level_downstream_dataset_ids
                )
                next_level_downstream_dataset_ids |= {
                    symlink.dataset_id for symlink in extra_symlink_groups
                } - all_dataset_ids
                symlink_groups += extra_symlink_groups

            if direction in {LineageDirectionV1.UPSTREAM, LineageDirectionV1.BOTH}:
                upstream_relations = await self._uow.io_dataset_relation.get_relations(
                    next_level_upstream_dataset_ids,
                    since=since,
                    until=until,
                    direction="UPSTREAM",
                )
                relations_by_id.update(
                    {(relation.in_dataset_id, relation.out_dataset_id): relation for relation in upstream_relations},
                )

                next_level_upstream_dataset_ids = {
                    relation.in_dataset_id for relation in upstream_relations
                } - all_dataset_ids

                extra_symlink_groups = await self._uow.dataset_symlink.get_symlink_groups(
                    next_level_upstream_dataset_ids
                )
                next_level_upstream_dataset_ids |= {
                    symlink.dataset_id for symlink in extra_symlink_groups
                } - all_dataset_ids
                symlink_groups += extra_symlink_groups

            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "[Level %d] Found %d datasets, %d IO relations",
                    level,
                    len(next_level_upstream_dataset_ids | next_level_downstream_dataset_ids),
                    len(relations_by_id),
                )

            all_dataset_ids |= next_level_upstream_dataset_ids
            all_dataset_ids |= next_level_downstream_dataset_ids

            result.io_dataset_relations.update(relations_by_id)
            depth -= 1
            level += 1

        datasets = await self._uow.dataset.list_by_ids(all_dataset_ids)
        result.datasets = {dataset.id: dataset for dataset in datasets}

        result.dataset_symlinks = {
            (pair.from_dataset_id, pair.to_dataset_id): pair for pair in _reconstruct_symlink_pairs(symlink_groups)
        }

        schema_ids: set[int] = set()
        for relation in result.io_dataset_relations.values():
            if relation.input_schema_id is not None:
                schema_ids.add(relation.input_schema_id)
            if relation.output_schema_id is not None:
                schema_ids.add(relation.output_schema_id)

        schemas = await self._uow.schema.list_by_ids(schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}
        for relation in result.io_dataset_relations.values():
            if relation.input_schema_id is not None:
                relation.input_schema = schemas_by_id.get(relation.input_schema_id)
            if relation.output_schema_id is not None:
                relation.output_schema = schemas_by_id.get(relation.output_schema_id)

        if include_column_lineage:
            column_lineage_result = await self._uow.column_lineage.list_by_dataset_pairs(
                result.io_dataset_relations.keys(),
                since,
                until,
            )
            column_lineage_relations: dict[tuple[int, int], list[ColumnLineageRow]] = defaultdict(list)
            for item in column_lineage_result:
                column_lineage_relations[(item.source_dataset_id, item.target_dataset_id)].append(item)
            result.column_lineage.update(column_lineage_relations)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "[Total] Found %d datasets, %d dataset symlinks, %d IO relations, %d column lineage",
                len(result.datasets),
                len(result.dataset_symlinks),
                len(result.io_dataset_relations),
                len(result.column_lineage),
            )
        return result

    def _build_column_lineage(
        self,
        data: list[ColumnLineageRow],
    ) -> dict[tuple[int, int], list[ColumnLineageRow]]:
        result = defaultdict(list)
        for relation in data:
            result[(relation.source_dataset_id, relation.target_dataset_id)].append(
                relation,
            )
        return result

    async def _fill_input_output_schemas(self, result: LineageServiceIntermediateResult):
        schema_ids = {input_.schema_id for input_ in result.inputs if input_.schema_id is not None} | {
            output.schema_id for output in result.outputs if output.schema_id is not None
        }

        schemas = await self._uow.schema.list_by_ids(schema_ids)
        schemas_by_id = {schema.id: schema for schema in schemas}

        for input_ in result.inputs:
            if input_.schema_id is not None:
                input_.schema = schemas_by_id.get(input_.schema_id)
        for output in result.outputs:
            if output.schema_id is not None:
                output.schema = schemas_by_id.get(output.schema_id)
