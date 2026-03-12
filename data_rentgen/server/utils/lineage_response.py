# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from data_rentgen.db.repositories.input import InputRow
from data_rentgen.db.repositories.output import OutputRow
from data_rentgen.server.schemas.v1 import (
    ColumnLineageInteractionTypeV1,
    DatasetResponseV1,
    DatasetSchemaV1,
    DirectLineageColumnRelationV1,
    IndirectLineageColumnRelationV1,
    JobResponseV1,
    LineageEntityKindV1,
    LineageEntityV1,
    LineageInputRelationV1,
    LineageNodesResponseV1,
    LineageOutputRelationV1,
    LineageParentRelationV1,
    LineageResponseV1,
    LineageSourceColumnV1,
    LineageSymlinkRelationV1,
    LocationResponseV1,
    OperationResponseV1,
    OutputTypeV1,
    RunResponseV1,
)
from data_rentgen.server.schemas.v1.lineage import (
    LineageRelationsResponseV1,
)

if TYPE_CHECKING:
    from uuid import UUID

    from data_rentgen.db.models.dataset import Dataset
    from data_rentgen.db.models.dataset_symlink import DatasetSymlink
    from data_rentgen.db.models.operation import Operation
    from data_rentgen.db.models.run import Run
    from data_rentgen.db.repositories.column_lineage import ColumnLineageRow
    from data_rentgen.db.repositories.io_dataset_relation import IODatasetRelationRow
    from data_rentgen.server.services.lineage import LineageServiceResult


def build_lineage_response(lineage: LineageServiceResult) -> LineageResponseV1:
    datasets = _get_datasets(lineage.datasets, lineage.outputs, lineage.inputs)
    jobs = {str(job.id): JobResponseV1.model_validate(job) for job in lineage.jobs.values()}
    runs = {run.id: RunResponseV1.model_validate(run) for run in lineage.runs.values()}
    operations = {op.id: OperationResponseV1.model_validate(op) for op in lineage.operations.values()}

    return LineageResponseV1(
        nodes=LineageNodesResponseV1(
            jobs=jobs,
            datasets=datasets,
            runs=runs,  # type: ignore[assignment, arg-type]
            operations=operations,  # type: ignore[assignment, arg-type]
        ),
        relations=LineageRelationsResponseV1(
            parents=_get_run_parent_relations(lineage.runs) + _get_operation_parent_relations(lineage.operations),
            ancestors=_get_jobs_hierarchy_chain(lineage.job_ancestor_relations)
            + _get_runs_hierarchy_chain(lineage.run_ancestor_relations),
            symlinks=_get_symlink_relations(lineage.dataset_symlinks),
            inputs=_get_input_relations(lineage.inputs),
            outputs=_get_output_relations(lineage.outputs),
            direct_column_lineage=_get_direct_column_lineage(lineage.column_lineage),
            indirect_column_lineage=_get_indirect_column_lineage(lineage.column_lineage),
        ),
    )


def build_lineage_response_with_dataset_granularity(lineage: LineageServiceResult) -> LineageResponseV1:
    datasets = _get_datasets_with_dataset_granularity(lineage.datasets, lineage.io_dataset_relations)
    return LineageResponseV1(
        nodes=LineageNodesResponseV1(datasets=datasets),
        relations=LineageRelationsResponseV1(
            symlinks=_get_symlink_relations(lineage.dataset_symlinks),
            inputs=_get_input_relations_with_dataset_granularity(lineage.io_dataset_relations),
            direct_column_lineage=_get_direct_column_lineage(lineage.column_lineage),
            indirect_column_lineage=_get_indirect_column_lineage(lineage.column_lineage),
        ),
    )


def _get_run_parent_relations(runs: dict[UUID, Run]) -> list[LineageParentRelationV1]:
    parents = []
    for run_id in sorted(runs):
        run = runs[run_id]
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(run.job_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
        )
        parents.append(relation)
    return parents


def _get_operation_parent_relations(operations: dict[UUID, Operation]) -> list[LineageParentRelationV1]:
    parents = []
    for operation_id in sorted(operations):
        operation = operations[operation_id]
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
        )
        parents.append(relation)
    return parents


def _get_symlink_relations(dataset_symlinks: dict[Any, DatasetSymlink]) -> list[LineageSymlinkRelationV1]:
    symlinks = []
    for key in sorted(dataset_symlinks):
        dataset_symlink = dataset_symlinks[key]
        relation = LineageSymlinkRelationV1(
            type=dataset_symlink.type,
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(dataset_symlink.from_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(dataset_symlink.to_dataset_id)),
        )
        symlinks.append(relation)
    return sorted(symlinks, key=lambda x: (x.from_.id, x.to.id))


def _get_input_relations(inputs: dict[Any, InputRow]) -> list[LineageInputRelationV1]:
    relations = []
    for input_ in inputs.values():
        if input_.operation_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input_.operation_id)
        elif input_.run_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=input_.run_id)
        elif input_.job_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(input_.job_id))

        relation = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(input_.dataset_id)),
            to=to,
            last_interaction_at=input_.created_at,
            num_bytes=input_.num_bytes,
            num_rows=input_.num_rows,
            num_files=input_.num_files,
        )
        relations.append(relation)

    return sorted(relations, key=lambda x: (x.to.kind, str(x.from_.id), str(x.to.id)))


def _get_output_relations(outputs: dict[Any, OutputRow]) -> list[LineageOutputRelationV1]:
    relations = []
    for output in outputs.values():
        if output.operation_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id)
        elif output.run_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=output.run_id)
        elif output.job_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(output.job_id))

        relation = LineageOutputRelationV1(
            types=[type_ for type_ in OutputTypeV1 if type_ & output.types_combined],  # type: ignore[operator]
            from_=from_,
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(output.dataset_id)),
            last_interaction_at=output.created_at,
            num_bytes=output.num_bytes,
            num_rows=output.num_rows,
            num_files=output.num_files,
        )
        relations.append(relation)

    return sorted(relations, key=lambda x: (x.from_.kind, str(x.from_.id), str(x.to.id)))


def _get_input_relations_with_dataset_granularity(
    io_dataset_relations: dict[Any, IODatasetRelationRow],
) -> list[LineageInputRelationV1]:
    relations = []
    for relation in io_dataset_relations.values():
        input_ = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(relation.in_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(relation.out_dataset_id)),
            last_interaction_at=relation.created_at,
        )

        relations.append(input_)
    return sorted(relations, key=lambda x: (str(x.from_.id), str(x.to.id)))


def _get_direct_column_lineage(column_lineage_by_source_target_id: dict[tuple, list[ColumnLineageRow]]):
    relations = []
    for (source_dataset_id, target_dataset_id), column_relations in column_lineage_by_source_target_id.items():
        column_lineage_relation = DirectLineageColumnRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(source_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(target_dataset_id)),
        )
        fields = defaultdict(list)
        for column_relation in column_relations:
            if column_relation.target_column:
                fields[column_relation.target_column].append(
                    LineageSourceColumnV1(
                        field=column_relation.source_column,
                        last_used_at=column_relation.last_used_at,
                        types=[
                            type_
                            for type_ in ColumnLineageInteractionTypeV1
                            if type_.value & column_relation.types_combined
                        ],
                    ),
                )
        if fields:
            column_lineage_relation.fields = fields  # type: ignore[assignment]
            relations.append(column_lineage_relation)
    return sorted(relations, key=lambda x: (str(x.from_.id), str(x.to.id)))


def _get_indirect_column_lineage(column_lineage_by_source_target_id: dict[tuple, list[ColumnLineageRow]]):
    relations = []
    for (source_dataset_id, target_dataset_id), column_relations in column_lineage_by_source_target_id.items():
        column_lineage_relation = IndirectLineageColumnRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(source_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(target_dataset_id)),
        )
        fields = [
            LineageSourceColumnV1(
                field=column_relation.source_column,
                last_used_at=column_relation.last_used_at,
                types=[
                    type_ for type_ in ColumnLineageInteractionTypeV1 if type_.value & column_relation.types_combined
                ],
            )
            for column_relation in column_relations
            if column_relation.target_column is None
        ]
        if fields:
            column_lineage_relation.fields = fields
            relations.append(column_lineage_relation)
    return sorted(relations, key=lambda x: (str(x.from_.id), str(x.to.id)))


def _get_latest_io_schema(dataset: Dataset, relations: list[OutputRow | InputRow]) -> DatasetSchemaV1 | None:
    relations = [
        relation for relation in relations if relation.dataset_id == dataset.id and relation.schema is not None
    ]
    if not relations:
        return None
    relations = sorted(relations, key=lambda relation: relation.created_at)
    oldest_relation, newest_relation = relations[0], relations[-1]

    dataset_schema = DatasetSchemaV1.model_validate(newest_relation.schema)
    if oldest_relation.schema_id == newest_relation.schema_id:
        dataset_schema.relevance_type = "EXACT_MATCH"
    else:
        dataset_schema.relevance_type = "LATEST_KNOWN"

    return dataset_schema


def _get_datasets(
    raw_datasets: dict[int, Dataset],
    outputs: dict[Any, OutputRow],
    inputs: dict[Any, InputRow],
) -> dict[str, DatasetResponseV1]:
    datasets: dict[str, DatasetResponseV1] = defaultdict()
    for dataset in raw_datasets.values():
        schema: DatasetSchemaV1 | None = None
        schema = _get_latest_io_schema(dataset, list(outputs.values())) or _get_latest_io_schema(
            dataset,
            list(inputs.values()),
        )
        datasets[str(dataset.id)] = DatasetResponseV1(
            id=str(dataset.id),
            location=LocationResponseV1.model_validate(dataset.location),
            name=dataset.name,
            schema=schema,
        )
    return datasets


def _get_io_by_dataset(
    io_relations: dict[tuple[int, int], IODatasetRelationRow],
) -> tuple[dict[int, list], dict[int, list]]:
    # Group inputs and outputs by dataset
    outputs, inputs = defaultdict(list), defaultdict(list)
    for (input_dataset_id, output_dataset_id), relation in io_relations.items():
        outputs[output_dataset_id].append(
            OutputRow(
                created_at=relation.created_at,
                operation_id=None,  # type: ignore[arg-type]
                run_id=None,  # type: ignore[arg-type]
                job_id=None,  # type: ignore[arg-type]
                dataset_id=output_dataset_id,
                schema_id=relation.output_schema_id,
                schema=relation.output_schema,
                schema_relevance_type=relation.output_schema_relevance_type,
                num_bytes=None,
                num_rows=None,
                num_files=None,
            ),
        )
        inputs[input_dataset_id].append(
            InputRow(
                created_at=relation.created_at,
                operation_id=None,  # type: ignore[arg-type]
                run_id=None,  # type: ignore[arg-type]
                job_id=None,  # type: ignore[arg-type]
                dataset_id=input_dataset_id,
                schema_id=relation.input_schema_id,
                schema=relation.input_schema,
                schema_relevance_type=relation.input_schema_relevance_type,
                num_bytes=None,
                num_rows=None,
                num_files=None,
            ),
        )
    return outputs, inputs


def _get_datasets_with_dataset_granularity(
    raw_datasets: dict[int, Dataset],
    io_relations: dict[tuple[int, int], IODatasetRelationRow],
) -> dict[str, DatasetResponseV1]:
    datasets: dict[str, DatasetResponseV1] = defaultdict()

    outputs_by_dataset_id, inputs_by_dataset_id = _get_io_by_dataset(io_relations)

    for dataset in raw_datasets.values():
        schema: DatasetSchemaV1 | None = None
        schema = _get_latest_io_schema(dataset, outputs_by_dataset_id[dataset.id]) or _get_latest_io_schema(
            dataset,
            inputs_by_dataset_id[dataset.id],
        )
        datasets[str(dataset.id)] = DatasetResponseV1(
            id=str(dataset.id),
            location=LocationResponseV1.model_validate(dataset.location),
            name=dataset.name,
            schema=schema,
        )
    return datasets


def _get_runs_hierarchy_chain(runs_relations: set[tuple[UUID, UUID]]) -> list[LineageParentRelationV1]:
    parents = []
    for parent_run_id, run_id in runs_relations:
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=parent_run_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run_id),
        )
        parents.append(relation)
    return sorted(parents, key=lambda x: (x.from_.id, x.to.id))


def _get_jobs_hierarchy_chain(jobs_relations: set[tuple[int, int]]) -> list[LineageParentRelationV1]:
    parents = []
    for parent_job_id, job_id in jobs_relations:
        relation = LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(parent_job_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(job_id)),
        )
        parents.append(relation)
    return sorted(parents, key=lambda x: (x.from_.id, x.to.id))
