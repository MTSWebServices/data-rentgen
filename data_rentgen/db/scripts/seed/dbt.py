# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from faker import Faker

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.db.scripts.seed.airflow import generate_airflow_run
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
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
    SQLQueryDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.utils.uuid import generate_new_uuid

LOCATIONS = {
    "local": LocationDTO(
        type="local",
        name="host01.companyname.com",
        addresses={"local://host01.companyname.com"},
    ),
    "clickhouse": LocationDTO(
        type="clickhouse",
        name="clickhouse01.companyname.com",
        addresses={"clickhouse://clickhouse01.companyname.com:9000"},
    ),
}

DATASETS = {
    "clickhouse_access_logs": DatasetDTO(
        name="app.access_logs",
        location=LOCATIONS["clickhouse"],
    ),
    "clickhouse_user_metrics": DatasetDTO(
        name="batch.user_metrics",
        location=LOCATIONS["clickhouse"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="owner.product"),
                value="DataPlans Store",
            ),
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="bronze",
            ),
        },
    ),
}

DATASET_SCHEMAS = {
    "clickhouse_access_logs": SchemaDTO(
        fields=[
            {"name": "id", "type": "Int64"},
            {"name": "user", "type": "String"},
            {"name": "timestamp", "type": "DateTime"},
            {"name": "client_ip", "type": "IPv4"},
        ],
    ),
    "clickhouse_user_metrics": SchemaDTO(
        fields=[
            {"name": "id", "type": "Int64"},
            {"name": "user", "type": "String"},
            {"name": "timestamp", "type": "DateTime"},
            {"name": "business_dt", "type": "Date"},
            {"name": "value", "type": "Float64"},
        ],
    ),
}


def generate_dbt_run(
    faker: Faker,
    start: datetime,
    end: datetime,
) -> BatchExtractionResult:
    run_created_at = faker.date_time_between(start, end, tzinfo=UTC)
    run_started_at = run_created_at + timedelta(minutes=faker.pyfloat(min_value=0, max_value=3))
    run_ended_at = run_started_at + timedelta(minutes=faker.pyfloat(min_value=30, max_value=35))
    parent_run = generate_airflow_run(
        "mart_layer_dag",
        "mart_layer_task_dbt",
        run_created_at - timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
        run_ended_at + timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
    )

    job = JobDTO(
        name="dbt-run-user_metrics",
        location=LOCATIONS["local"],
        type=JobTypeDTO(type="DBT_JOB"),
        parent_job=parent_run.job,
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="dbt.version"),
                value="1.9.4",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.43.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_client.version"),
                value="1.43.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="environment"),
                value="production",
            ),
        },
    )

    run_id = generate_new_uuid(run_created_at)
    run = RunDTO(
        id=run_id,
        job=job,
        parent_run=parent_run,
        status=RunStatusDTO.SUCCEEDED,
        external_id=str(uuid4()),
        started_at=run_started_at,
        ended_at=run_ended_at,
    )
    result = BatchExtractionResult()
    result.add_job(job)
    result.add_run(run)

    for generator in [dbt_raw_to_mart]:
        operation, inputs, outputs, column_lineage = generator(faker, run)
        result.add_operation(operation)
        for input_ in inputs:
            result.add_input(input_)
        for output in outputs:
            result.add_output(output)
        for lineage in column_lineage:
            result.add_column_lineage(lineage)
    return result


def dbt_raw_to_mart(
    faker: Faker,
    parent_run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    started_at: datetime = parent_run.started_at  # type: ignore[assignment]
    operation_started_at = started_at + timedelta(seconds=faker.pyfloat(min_value=0, max_value=5))
    operation_ended_at = operation_started_at + timedelta(minutes=faker.pyfloat(min_value=20, max_value=30))
    operation_id = generate_new_uuid(operation_started_at)
    operation = OperationDTO(
        id=operation_id,
        run=parent_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=operation_started_at,
        ended_at=operation_ended_at,
        name="DBT_MODEL",
        sql_query=SQLQueryDTO(
            query=textwrap.dedent(
                f"""
                WITH raw_data AS (
                    SELECT
                        id,
                        user,
                        timestamp,
                        client_ip,
                    FROM app.access_logs
                    WHERE timestamp >= '{operation_started_at.date()}'
                )
                SELECT
                    raw_data.user,
                    raw_data.timestamp,
                    (
                        SELECT COUNT(*)
                        FROM raw_data AS nested
                        WHERE raw_data.user = nested.user
                    ) as value
                FROM raw_data
                """,  # noqa: S608
            ).strip(),
        ),
    )
    input_ = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["clickhouse_access_logs"],
        schema=DATASET_SCHEMAS["clickhouse_access_logs"],
        # dbt doesn't know about input size
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["clickhouse_user_metrics"],
        schema=DATASET_SCHEMAS["clickhouse_user_metrics"],
        type=OutputTypeDTO.APPEND,
        num_bytes=faker.pyint(min_value=2**10, max_value=2**32),
        num_rows=faker.pyint(min_value=10**3, max_value=10**6),
    )
    column_lineage = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input_.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="user",
                target_column="user",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="timestamp",
                target_column="timestamp",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="timestamp",
                type=DatasetColumnRelationTypeDTO.FILTER,
            ),
            DatasetColumnRelationDTO(
                source_column="user",
                type=DatasetColumnRelationTypeDTO.JOIN,
            ),
        ],
    )
    return operation, [input_], [output], [column_lineage]
