from __future__ import annotations

from itertools import groupby
from typing import TYPE_CHECKING, Literal

from data_rentgen.db.models import (
    Input,
    Output,
)
from data_rentgen.server.schemas.v1.lineage import OutputTypeV1

if TYPE_CHECKING:
    from collections.abc import Collection
    from datetime import datetime

    from data_rentgen.db.models import (
        Address,
        Dataset,
        DatasetSymlink,
        Job,
        Location,
        Operation,
        PersonalToken,
        Run,
        Schema,
        Tag,
        TagValue,
        User,
    )
    from data_rentgen.db.repositories.input import InputRow
    from data_rentgen.db.repositories.output import OutputRow


def format_datetime(value: datetime):
    result = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # pydantic datetime formatter quirk
    return result.replace(".000000", "")


def run_parent_to_json(run: Run):
    return {
        "from": {"kind": "JOB", "id": str(run.job_id)},
        "to": {"kind": "RUN", "id": str(run.id)},
    }


def run_parents_to_json(runs: list[Run]):
    return [run_parent_to_json(run) for run in sorted(runs, key=lambda x: x.id)]


def operation_parent_to_json(operation: Operation):
    return {
        "from": {"kind": "RUN", "id": str(operation.run_id)},
        "to": {"kind": "OPERATION", "id": str(operation.id)},
    }


def operation_parents_to_json(operations: list[Operation]):
    return [operation_parent_to_json(run) for run in sorted(operations, key=lambda x: x.id)]


def run_ancestor_to_json(run: Run):
    return {
        "from": {"kind": "RUN", "id": str(run.parent_run_id)},
        "to": {"kind": "RUN", "id": str(run.id)},
    }


def runs_ancestors_to_json(runs: list[Run]):
    results = [run_ancestor_to_json(run) for run in runs if run.parent_run_id]
    return sorted(results, key=lambda x: (x["from"]["id"], x["to"]["id"]))


def job_ancestor_to_json(job: Job):
    return {
        "from": {"kind": "JOB", "id": str(job.parent_job_id)},
        "to": {"kind": "JOB", "id": str(job.id)},
    }


def jobs_ancestors_to_json(jobs: list[Job]):
    results = [job_ancestor_to_json(job) for job in jobs if job.parent_job_id]
    return sorted(results, key=lambda x: (x["from"]["id"], x["to"]["id"]))


def symlink_to_json(symlink: DatasetSymlink):
    return {
        "from": {"kind": "DATASET", "id": str(symlink.from_dataset_id)},
        "to": {"kind": "DATASET", "id": str(symlink.to_dataset_id)},
        "type": symlink.type.value,
    }


def symlinks_to_json(symlinks: list[DatasetSymlink]):
    return [
        symlink_to_json(symlink)
        for symlink in sorted(symlinks, key=lambda x: (str(x.from_dataset_id), str(x.to_dataset_id)))
    ]


def schema_to_json(schema: Schema, schema_relevance_type: str):
    return {
        "id": str(schema.id),
        "fields": [
            {
                "description": None,
                "fields": [],
                **field,
            }
            for field in schema.fields
        ],
        "relevance_type": schema_relevance_type,
    }


def input_to_json(input: InputRow | Input, granularity: Literal["OPERATION", "RUN", "JOB"]):
    if granularity == "OPERATION":
        to = {"kind": "OPERATION", "id": str(input.operation_id)}
    elif granularity == "RUN":
        to = {"kind": "RUN", "id": str(input.run_id)}
    else:
        to = {"kind": "JOB", "id": str(input.job_id)}

    return {
        "from": {"kind": "DATASET", "id": str(input.dataset_id)},
        "to": to,
        "num_bytes": input.num_bytes,
        "num_rows": input.num_rows,
        "num_files": input.num_files,
        "last_interaction_at": format_datetime(input.created_at),
    }


def inputs_to_json(inputs: list[InputRow | Input], granularity: Literal["OPERATION", "RUN", "JOB"]):
    results = [input_to_json(input_, granularity) for input_ in inputs]
    return sorted(
        results,
        key=lambda x: (x["from"]["id"], x["to"]["id"]),
    )


def output_to_json(output: OutputRow | Output, granularity: Literal["OPERATION", "RUN", "JOB"]):
    if granularity == "OPERATION":
        from_ = {"kind": "OPERATION", "id": str(output.operation_id)}
    elif granularity == "RUN":
        from_ = {"kind": "RUN", "id": str(output.run_id)}
    else:
        from_ = {"kind": "JOB", "id": str(output.job_id)}

    if isinstance(output, Output):
        types = [output.type.name]
    else:
        types = [type_.name for type_ in OutputTypeV1 if type_ & output.types_combined]
    return {
        "from": from_,
        "to": {"kind": "DATASET", "id": str(output.dataset_id)},
        "types": types,
        "num_bytes": output.num_bytes,
        "num_rows": output.num_rows,
        "num_files": output.num_files,
        "last_interaction_at": format_datetime(output.created_at),
    }


def outputs_to_json(outputs: list[OutputRow | Output], granularity: Literal["OPERATION", "RUN", "JOB"]):
    results = [output_to_json(output, granularity) for output in outputs]
    return sorted(
        results,
        key=lambda x: (x["from"]["id"], x["to"]["id"]),
    )


def address_to_json(address: Address):
    return {"url": address.url}


def location_to_json(location: Location):
    return {
        "id": str(location.id),
        "name": location.name,
        "type": location.type,
        "addresses": [address_to_json(address) for address in location.addresses],
        "external_id": location.external_id,
    }


def locations_to_json(locations: list[Location]):
    return {str(location.id): location_to_json(location) for location in locations}


def _get_dataset_schema(dataset: Dataset, outputs: list[OutputRow | Output], inputs: list[InputRow | Input]):
    outputs = sorted(outputs, key=lambda output: output.schema_id, reverse=True)
    inputs = sorted(inputs, key=lambda input: input.schema_id, reverse=True)
    schema = next((output.schema for output in outputs if output.dataset_id == dataset.id), None) or next(
        (input.schema for input in inputs if input.dataset_id == dataset.id),
        None,
    )

    if not schema:
        return None

    return schema_to_json(schema, "EXACT_MATCH")


def tag_to_json(tag: Tag, values: list[TagValue] | None = None) -> dict:
    values = values or tag.tag_values
    return {
        "id": tag.id,
        "name": tag.name,
        "values": [{"id": tv.id, "value": tv.value} for tv in sorted(values, key=lambda tv: tv.value)],
    }


def tag_values_to_json(tag_values: Collection[TagValue]) -> list[dict]:
    sorted_tag_values = sorted(tag_values, key=lambda tv: tv.tag.name)
    tags = []
    for tag, group in groupby(sorted_tag_values, key=lambda tv: tv.tag):
        tags.append(tag_to_json(tag, values=list(group)))
    return tags


def dataset_to_json(
    dataset: Dataset,
    outputs: list[OutputRow | Output] | None = None,
    inputs: list[InputRow | Input] | None = None,
):
    schema = None
    if outputs or inputs:
        schema = _get_dataset_schema(dataset, outputs, inputs)
    return {
        "id": str(dataset.id),
        "name": dataset.name,
        "location": location_to_json(dataset.location),
        "schema": schema,
    }


def datasets_to_json(
    datasets: list[Dataset],
    outputs: list[OutputRow | Output] | None = None,
    inputs: list[InputRow | Input] | None = None,
):
    if inputs is None:
        inputs = []
    if outputs is None:
        outputs = []
    return {str(dataset.id): dataset_to_json(dataset, outputs, inputs) for dataset in datasets}


def job_to_json(job: Job):
    return {
        "id": str(job.id),
        "parent_job_id": str(job.parent_job_id) if job.parent_job_id else None,
        "name": job.name,
        "type": job.type,
        "location": location_to_json(job.location),
    }


def jobs_to_json(jobs: list[Job]):
    return {str(job.id): job_to_json(job) for job in jobs}


def user_to_json(user: User):
    return {"name": user.name}


def run_to_json(run: Run):
    return {
        "id": str(run.id),
        "job_id": str(run.job_id),
        "created_at": format_datetime(run.created_at),
        "parent_run_id": str(run.parent_run_id) if run.parent_run_id else None,
        "status": run.status.name,
        "external_id": run.external_id,
        "attempt": run.attempt,
        "persistent_log_url": run.persistent_log_url,
        "running_log_url": run.running_log_url,
        "started_at": format_datetime(run.started_at) if run.started_at else None,
        "started_by_user": user_to_json(run.started_by_user) if run.started_by_user else None,
        "start_reason": run.start_reason.value if run.start_reason else None,
        "ended_at": format_datetime(run.ended_at) if run.ended_at else None,
        "end_reason": run.end_reason,
        "expected_start_at": format_datetime(run.expected_start_at) if run.expected_start_at else None,
        "expected_end_at": format_datetime(run.expected_end_at) if run.expected_end_at else None,
    }


def runs_to_json(runs: list[Run]):
    return {str(run.id): run_to_json(run) for run in runs}


def operation_to_json(operation: Operation):
    return {
        "id": str(operation.id),
        "created_at": format_datetime(operation.created_at),
        "run_id": str(operation.run_id),
        "name": operation.name,
        "status": operation.status.name,
        "type": operation.type.value,
        "position": operation.position,
        "group": operation.group,
        "description": operation.description,
        "sql_query": operation.sql_query,
        "started_at": format_datetime(operation.started_at) if operation.started_at else None,
        "ended_at": format_datetime(operation.ended_at) if operation.ended_at else None,
    }


def operations_to_json(operations: list[Operation]):
    return {str(operation.id): operation_to_json(operation) for operation in operations}


def personal_token_to_json(user_token: PersonalToken):
    return {
        "id": str(user_token.id),
        "name": user_token.name,
        "scopes": user_token.scopes,
        "since": user_token.since.isoformat(),  # type: ignore[union-attr]
        "until": user_token.until.isoformat(),  # type: ignore[union-attr]
    }
