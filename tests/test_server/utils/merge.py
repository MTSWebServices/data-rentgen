from collections.abc import Callable, Sequence

from data_rentgen.db.models import Input, Output
from data_rentgen.db.repositories.input import InputRow
from data_rentgen.db.repositories.output import OutputRow


def sum_or_null(a: int | None, b: int | None) -> int | None:
    if a is None or b is None:
        return None

    return a + b


def merge_inputs(inputs: Sequence[Input], get_key: Callable[[Input], tuple]) -> list[InputRow]:
    if not inputs:
        return []

    merged_inputs = {}
    for raw_io in inputs:
        key = get_key(raw_io)
        merged_io = merged_inputs.get(key)

        if not merged_io:
            merged_inputs[key] = merged_io = InputRow(
                operation_id=raw_io.operation_id,
                run_id=raw_io.run_id,
                job_id=raw_io.job_id,
                dataset_id=raw_io.dataset_id,
                created_at=raw_io.created_at,
                num_bytes=raw_io.num_bytes,
                num_files=raw_io.num_files,
                num_rows=raw_io.num_rows,
                schema_id=raw_io.schema_id,
                schema=raw_io.schema,
                schema_relevance_type="EXACT_MATCH" if raw_io.schema else None,
            )
        else:
            merged_io.created_at = max(merged_io.created_at, raw_io.created_at)
            merged_io.num_bytes = sum_or_null(merged_io.num_bytes, raw_io.num_bytes)
            merged_io.num_files = sum_or_null(merged_io.num_files, raw_io.num_files)
            merged_io.num_rows = sum_or_null(merged_io.num_rows, raw_io.num_rows)

            merged_io.schema_id = merged_io.schema_id or raw_io.schema_id
            if (
                merged_io.schema_id is not None
                and raw_io.schema_id is not None
                and merged_io.schema_id != raw_io.schema_id
            ):
                schema_id = max(merged_io.schema_id, raw_io.schema_id)
                merged_io.schema = merged_io.schema if schema_id == merged_io.schema_id else raw_io.schema
                merged_io.schema_id = schema_id
                merged_io.schema_relevance_type = "LATEST_KNOWN"

    return [input_ for _, input_ in sorted(merged_inputs.items(), key=lambda x: x[0])]


def merge_outputs(outputs: Sequence[Output], get_key: Callable[[Output], tuple]) -> list[OutputRow]:
    if not outputs:
        return []
    merged_outputs = {}

    for raw_io in outputs:
        key = get_key(raw_io)
        merged_io = merged_outputs.get(key)

        if not merged_io:
            merged_outputs[key] = merged_io = OutputRow(
                operation_id=raw_io.operation_id,
                run_id=raw_io.run_id,
                job_id=raw_io.job_id,
                dataset_id=raw_io.dataset_id,
                created_at=raw_io.created_at,
                num_bytes=raw_io.num_bytes,
                num_files=raw_io.num_files,
                num_rows=raw_io.num_rows,
                schema_id=raw_io.schema_id,
                schema=raw_io.schema,
                schema_relevance_type="EXACT_MATCH" if raw_io.schema else None,
                types_combined=raw_io.type,
            )
        else:
            merged_io.created_at = max(merged_io.created_at, raw_io.created_at)
            merged_io.num_bytes = sum_or_null(merged_io.num_bytes, raw_io.num_bytes)
            merged_io.num_files = sum_or_null(merged_io.num_files, raw_io.num_files)
            merged_io.num_rows = sum_or_null(merged_io.num_rows, raw_io.num_rows)

            merged_io.schema_id = merged_io.schema_id or raw_io.schema_id
            if (
                merged_io.schema_id is not None
                and raw_io.schema_id is not None
                and merged_io.schema_id != raw_io.schema_id
            ):
                schema_id = max(merged_io.schema_id, raw_io.schema_id)
                merged_io.schema = merged_io.schema if schema_id == merged_io.schema_id else raw_io.schema
                merged_io.schema_id = schema_id
                merged_io.schema_relevance_type = "LATEST_KNOWN"

            if merged_io.types_combined is not None:
                merged_io.types_combined |= raw_io.type
            else:
                merged_io.types_combined = raw_io.type

    return [output_ for _, output_ in sorted(merged_outputs.items(), key=lambda x: x[0])]


def merge_inputs_by_job(inputs: list[Input]) -> list[InputRow]:
    return merge_inputs(inputs, get_key=lambda x: (x.dataset_id, 0, x.job_id))


def merge_inputs_by_run(inputs: list[Input]) -> list[InputRow]:
    return merge_inputs(inputs, get_key=lambda x: (x.dataset_id, 1, x.run_id))


def merge_outputs_by_job(outputs: list[Output]) -> list[OutputRow]:
    return merge_outputs(outputs, get_key=lambda x: (0, x.job_id, x.dataset_id))


def merge_outputs_by_run(outputs: list[Output]) -> list[OutputRow]:
    return merge_outputs(outputs, get_key=lambda x: (1, x.run_id.int, x.dataset_id))
