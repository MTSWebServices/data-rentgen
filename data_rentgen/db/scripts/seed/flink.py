# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from faker import Faker

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetDTO,
    InputDTO,
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    OutputDTO,
    OutputTypeDTO,
    RunDTO,
    RunStatusDTO,
    SchemaDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.utils.uuid import generate_new_uuid

LOCATIONS = {
    "flink": LocationDTO(
        type="https",
        name="flink01.companyname.com",
        addresses={"https://flink01.hadoop.companyname.com"},
    ),
    "postgres": LocationDTO(
        type="postgres",
        name="postgres01.companyname.com:5432",
        addresses={"postgres://postgres01.companyname.com:5432"},
    ),
    "kafka": LocationDTO(
        type="kafka",
        name="kafka01.companyname.com:9092",
        addresses={"kafka://kafka01.companyname.com:9092"},
    ),
}

DATASETS = {
    "postgres_user_metrics": DatasetDTO(
        name="streaming.user_metrics",
        location=LOCATIONS["postgres"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="owner.product"),
                value="Calls History",
            ),
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="bronze",
            ),
        },
    ),
    "kafka_user_metrics": DatasetDTO(
        name="user_metrics",
        location=LOCATIONS["kafka"],
    ),
}

DATASET_SCHEMAS = {
    "kafka_user_metrics": SchemaDTO(
        fields=[
            {"name": "topic", "type": "string"},
            {"name": "partition", "type": "integer"},
            {"name": "key", "type": "binary"},
            {"name": "value", "type": "binary"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "timestampType", "type": "integer"},
        ],
    ),
    "postgres_user_metrics": SchemaDTO(
        fields=[
            {"name": "id", "type": "int"},
            {"name": "user", "type": "varchar"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "business_dt", "type": "date"},
            {"name": "value", "type": "double"},
        ],
    ),
}


def generate_flink_run(
    faker: Faker,
    start: datetime,
    end: datetime,
) -> BatchExtractionResult:
    job = JobDTO(
        name="user_metrics_flow",
        location=LOCATIONS["flink"],
        type=JobTypeDTO(type="FLINK_JOB"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="flink.version"),
                value="2.2.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.43.0",
            ),
            # no custom tags support for now
            # https://github.com/OpenLineage/OpenLineage/issues/4280
        },
    )
    address = next(iter(job.location.addresses))

    run_created_at = faker.date_time_between(start, end, tzinfo=UTC)
    run_id = generate_new_uuid(run_created_at)
    external_id = str(uuid4())
    run = RunDTO(
        id=run_id,
        job=job,
        status=RunStatusDTO.STARTED,
        external_id=external_id,
        running_log_url=f"{address}/#/job/running/{external_id}",
        persistent_log_url=f"{address}/#/job/completed/{external_id}",
        started_at=run_created_at,
    )
    result = BatchExtractionResult()
    result.add_job(job)
    result.add_run(run)

    for generator in [kafka_to_postgres]:
        operation, inputs, outputs, column_lineage = generator(run)
        result.add_operation(operation)
        for input_ in inputs:
            result.add_input(input_)
        for output in outputs:
            result.add_output(output)
        for lineage in column_lineage:
            result.add_column_lineage(lineage)
    return result


def kafka_to_postgres(
    run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    created_at: datetime = run.started_at  # type: ignore[assignment]
    operation_id = generate_new_uuid(created_at)
    operation = OperationDTO(
        id=operation_id,
        run=run,
        status=OperationStatusDTO.STARTED,
        type=OperationTypeDTO.STREAMING,
        started_at=created_at,
        name="kafka_to_postgres",
    )
    input_ = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["kafka_user_metrics"],
        schema=DATASET_SCHEMAS["kafka_user_metrics"],
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["postgres_user_metrics"],
        schema=DATASET_SCHEMAS["postgres_user_metrics"],
        type=OutputTypeDTO.APPEND,
    )
    # Flink doesn't send column lineage for now
    return operation, [input_], [output], []
