# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

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
    from collections.abc import Sequence
    from uuid import UUID

    from data_rentgen.db.models import Dataset, DatasetSymlinkGroup, Operation, Run, Schema
    from data_rentgen.db.repositories.column_lineage import ColumnLineageRow
    from data_rentgen.db.repositories.io_dataset_relation import IODatasetRelationRow
    from data_rentgen.server.services.lineage import LineageServiceResult


def build_lineage_response(lineage: LineageServiceResult) -> LineageResponseV1:
    jobs = {str(job.id): JobResponseV1.model_validate(job) for job in lineage.jobs}
    runs = {run.id: RunResponseV1.model_validate(run) for run in lineage.runs}
    operations = {op.id: OperationResponseV1.model_validate(op) for op in lineage.operations}
    datasets = _get_datasets(lineage.datasets, lineage.outputs, lineage.inputs)

    return LineageResponseV1(
        nodes=LineageNodesResponseV1(
            jobs=jobs,
            datasets=datasets,
            runs=runs,
            operations=operations,
        ),
        relations=LineageRelationsResponseV1(
            parents=_get_jobs_ancestor_relations(lineage.job_ancestor_relations)
            + _get_run_ancestor_relations(lineage.run_ancestor_relations)
            + _get_run_parent_relations(lineage.runs)
            + _get_operation_parent_relations(lineage.operations),
            symlinks=_get_symlink_relations(lineage.symlink_groups),
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
            symlinks=_get_symlink_relations(lineage.symlink_groups),
            inputs=_get_input_relations_with_dataset_granularity(lineage.io_dataset_relations),
            direct_column_lineage=_get_direct_column_lineage(lineage.column_lineage),
            indirect_column_lineage=_get_indirect_column_lineage(lineage.column_lineage),
        ),
    )


def _get_run_parent_relations(runs: list[Run]) -> list[LineageParentRelationV1]:
    return [
        LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(run.job_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run.id),
        )
        for run in runs
    ]


def _get_operation_parent_relations(operations: list[Operation]) -> list[LineageParentRelationV1]:
    return [
        LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=operation.run_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=operation.id),
        )
        for operation in operations
    ]


def _get_symlink_relations(symlink_groups: list[DatasetSymlinkGroup]) -> list[LineageSymlinkRelationV1]:
    members_by_fingerprint = defaultdict(list)
    for symlink_group in symlink_groups:
        members_by_fingerprint[symlink_group.fingerprint].append(symlink_group)

    pairs = {}
    for members in members_by_fingerprint.values():
        for from_ in members:
            for to in members:
                if from_.dataset_id == to.dataset_id:
                    continue

                pairs[(from_.dataset_id, to.dataset_id)] = LineageSymlinkRelationV1(
                    type=to.type,
                    from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(from_.dataset_id)),
                    to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(to.dataset_id)),
                )
    # symlink_groups are appended using +=, so sorting fetched from DB is lost
    return [pair for _, pair in sorted(pairs.items(), key=lambda item: item[0])]


def _get_input_relations(inputs: list[InputRow]) -> list[LineageInputRelationV1]:
    relations = {}
    for input_ in inputs:
        # inputs may be merged using +=, so sorting is not preserved.
        # also this list may contain duplicates

        if input_.operation_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=input_.operation_id)
            key = (2, input_.dataset_id, input_.operation_id.int)
        elif input_.run_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=input_.run_id)
            key = (1, input_.dataset_id, input_.run_id.int)
        elif input_.job_id is not None:
            to = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(input_.job_id))
            key = (0, input_.dataset_id, input_.job_id)

        if key in relations:
            continue

        relations[key] = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(input_.dataset_id)),
            to=to,
            last_interaction_at=input_.created_at,
            num_bytes=input_.num_bytes,
            num_rows=input_.num_rows,
            num_files=input_.num_files,
        )

    # inputs are appended using +=, so sorting fetched from DB is lost
    return [relation for _, relation in sorted(relations.items(), key=lambda item: item[0])]


def _get_output_relations(outputs: list[OutputRow]) -> list[LineageOutputRelationV1]:
    relations = {}
    for output in outputs:
        # outputs may be merged using +=, so sorting is not preserved.
        # also this list may contain duplicates

        if output.operation_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.OPERATION, id=output.operation_id)
            key = (2, output.operation_id.int, output.dataset_id)
        elif output.run_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.RUN, id=output.run_id)
            key = (1, output.run_id.int, output.dataset_id)
        elif output.job_id is not None:
            from_ = LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(output.job_id))
            key = (0, output.job_id, output.dataset_id)

        if key in relations:
            continue

        relations[key] = LineageOutputRelationV1(
            types=[type_ for type_ in OutputTypeV1 if type_ & output.types_combined],  # type: ignore[operator]
            from_=from_,
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(output.dataset_id)),
            last_interaction_at=output.created_at,
            num_bytes=output.num_bytes,
            num_rows=output.num_rows,
            num_files=output.num_files,
        )

    # outputs are appended using +=, so sorting fetched from DB is lost
    return [relation for _, relation in sorted(relations.items(), key=lambda item: item[0])]


def _get_input_relations_with_dataset_granularity(
    io_dataset_relations: list[IODatasetRelationRow],
) -> list[LineageInputRelationV1]:
    result = {}
    for relation in io_dataset_relations:
        key = (relation.in_dataset_id, relation.out_dataset_id)
        if key in result:
            continue

        result[key] = LineageInputRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(relation.in_dataset_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(relation.out_dataset_id)),
            last_interaction_at=relation.created_at,
        )
    # inputs are appended using +=, so sorting fetched from DB is lost
    return [relation for _, relation in sorted(result.items(), key=lambda item: item[0])]


def _get_direct_column_lineage(column_lineage: list[ColumnLineageRow]) -> list[DirectLineageColumnRelationV1]:
    relations: dict[tuple[int, int], DirectLineageColumnRelationV1] = {}
    for item in column_lineage:
        if not item.target_column:
            # indirect column lineage
            continue

        key = (item.source_dataset_id, item.target_dataset_id)
        column_lineage_relation = relations.get(key)
        if column_lineage_relation is None:
            column_lineage_relation = relations[key] = DirectLineageColumnRelationV1(
                from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(item.source_dataset_id)),
                to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(item.target_dataset_id)),
            )

        source_field = LineageSourceColumnV1(
            field=item.source_column,
            last_used_at=item.last_used_at,
            types=[type_ for type_ in ColumnLineageInteractionTypeV1 if type_.value & item.types_combined],
        )

        column_lineage_relation.fields[item.target_column].append(source_field)

    return list(relations.values())


def _get_indirect_column_lineage(
    column_lineage_by_source_target_id: list[ColumnLineageRow],
) -> list[IndirectLineageColumnRelationV1]:
    relations: dict[tuple[int, int], IndirectLineageColumnRelationV1] = {}
    for item in column_lineage_by_source_target_id:
        if item.target_column:
            # direct column lineage
            continue

        key = (item.source_dataset_id, item.target_dataset_id)
        column_lineage_relation = relations.get(key)
        if column_lineage_relation is None:
            column_lineage_relation = relations[key] = IndirectLineageColumnRelationV1(
                from_=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(item.source_dataset_id)),
                to=LineageEntityV1(kind=LineageEntityKindV1.DATASET, id=str(item.target_dataset_id)),
            )

        source_field = LineageSourceColumnV1(
            field=item.source_column,
            last_used_at=item.last_used_at,
            types=[type_ for type_ in ColumnLineageInteractionTypeV1 if type_.value & item.types_combined],
        )
        column_lineage_relation.fields.append(source_field)

    return list(relations.values())


def _get_latest_io_schema(relations: Sequence[OutputRow | InputRow]) -> DatasetSchemaV1 | None:
    oldest_schema: Schema | None = None
    newest_schema: Schema | None = None
    for relation in sorted(relations, key=lambda relation: (relation.created_at, relation.schema_id or 0)):
        if relation.schema is None:
            continue
        if oldest_schema is None:
            oldest_schema = relation.schema
        newest_schema = relation.schema

    if oldest_schema is None or newest_schema is None:
        return None

    result = DatasetSchemaV1.model_validate(newest_schema)
    if oldest_schema.id == newest_schema.id:
        result.relevance_type = "EXACT_MATCH"
    else:
        result.relevance_type = "LATEST_KNOWN"
    return result


def _get_datasets(
    raw_datasets: list[Dataset],
    outputs: list[OutputRow],
    inputs: list[InputRow],
) -> dict[str, DatasetResponseV1]:
    outputs_dict = defaultdict(list)
    for output in outputs:
        outputs_dict[output.dataset_id].append(output)

    inputs_dict = defaultdict(list)
    for input_ in inputs:
        inputs_dict[input_.dataset_id].append(input_)

    return {
        str(dataset.id): DatasetResponseV1(
            id=str(dataset.id),
            location=LocationResponseV1.model_validate(dataset.location),
            name=dataset.name,
            external_id=dataset.external_id,
            external_url=dataset.external_url,
            schema=(
                _get_latest_io_schema(outputs_dict.get(dataset.id, []))
                or _get_latest_io_schema(inputs_dict.get(dataset.id, []))
            ),
        )
        for dataset in raw_datasets
    }


def _get_io_by_dataset(
    io_relations: list[IODatasetRelationRow],
) -> tuple[dict[int, list], dict[int, list]]:
    # Group inputs and outputs by dataset
    outputs, inputs = defaultdict(list), defaultdict(list)
    for relation in io_relations:
        outputs[relation.out_dataset_id].append(
            OutputRow(
                created_at=relation.created_at,
                operation_id=None,
                run_id=None,
                job_id=0,
                dataset_id=relation.out_dataset_id,
                schema_id=relation.output_schema_id,
                schema=relation.output_schema,
                schema_relevance_type=relation.output_schema_relevance_type,
                num_bytes=None,
                num_rows=None,
                num_files=None,
            ),
        )
        inputs[relation.in_dataset_id].append(
            InputRow(
                created_at=relation.created_at,
                operation_id=None,
                run_id=None,
                job_id=0,
                dataset_id=relation.in_dataset_id,
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
    raw_datasets: list[Dataset],
    io_relations: list[IODatasetRelationRow],
) -> dict[str, DatasetResponseV1]:
    outputs_dict, inputs_dict = _get_io_by_dataset(io_relations)
    return {
        str(dataset.id): DatasetResponseV1(
            id=str(dataset.id),
            location=LocationResponseV1.model_validate(dataset.location),
            name=dataset.name,
            external_id=dataset.external_id,
            external_url=dataset.external_url,
            schema=(
                _get_latest_io_schema(outputs_dict.get(dataset.id, []))
                or _get_latest_io_schema(inputs_dict.get(dataset.id, []))
            ),
        )
        for dataset in raw_datasets
    }


def _get_run_ancestor_relations(runs_relations: list[tuple[UUID, UUID]]) -> list[LineageParentRelationV1]:
    return [
        LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=parent_run_id),
            to=LineageEntityV1(kind=LineageEntityKindV1.RUN, id=run_id),
        )
        for parent_run_id, run_id in runs_relations
    ]


def _get_jobs_ancestor_relations(jobs_relations: list[tuple[int, int]]) -> list[LineageParentRelationV1]:
    return [
        LineageParentRelationV1(
            from_=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(parent_job_id)),
            to=LineageEntityV1(kind=LineageEntityKindV1.JOB, id=str(job_id)),
        )
        for parent_job_id, job_id in jobs_relations
    ]
