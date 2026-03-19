# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap
from datetime import UTC, datetime, timedelta

from faker import Faker

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.db.scripts.seed.airflow import generate_airflow_run
from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
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
    UserDTO,
)
from data_rentgen.utils.uuid import generate_new_uuid

LOCATIONS = {
    "local": LocationDTO(
        type="local",
        name="host01.companyname.com",
        addresses={"local://host01.companyname.com"},
    ),
    "hdfs": LocationDTO(
        type="hdfs",
        name="hadoop.companyname.com",
        addresses={
            "hdfs://mn01.hadoop.companyname.com:8020",
            "hdfs://mn02.hadoop.companyname.com:8020",
            "webhdfs://mn01.hadoop.companyname.com:9870",
            "webhdfs://mn02.hadoop.companyname.com:9870",
        },
    ),
    "hive_metastore": LocationDTO(
        type="hive",
        name="hadoop.companyname.com",
        addresses={
            "hive://hive01.hadoop.companyname.com:9083",
            "hive://hive02.hadoop.companyname.com:9083",
        },
    ),
    "postgres": LocationDTO(
        type="postgres",
        name="postgres01.companyname.com:5432",
        addresses={"postgres://postgres01.companyname.com:5432"},
    ),
    "clickhouse": LocationDTO(
        type="clickhouse",
        name="clickhouse01.companyname.com",
        addresses={"clickhouse://clickhouse01.companyname.com:8123"},
    ),
}

DATASETS = {
    "hive_raw_user_metrics": DatasetDTO(
        name="raw.user_metrics",
        location=LOCATIONS["hive_metastore"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="silver",
            ),
            TagValueDTO(
                tag=TagDTO(name="owner.team"),
                value="DE Team 1",
            ),
        },
    ),
    "hdfs_raw_user_metrics": DatasetDTO(
        name="/user/hive/warehouse/raw.db/user_metrics",
        location=LOCATIONS["hdfs"],
    ),
    "postgres_user_metrics": DatasetDTO(
        name="streaming.user_metrics",
        location=LOCATIONS["postgres"],
    ),
    "clickhouse_user_metrics": DatasetDTO(
        name="batch.user_metrics",
        location=LOCATIONS["clickhouse"],
    ),
}

DATASET_SYMLINKS = [
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hive_raw_user_metrics"],
        to_dataset=DATASETS["hdfs_raw_user_metrics"],
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    ),
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hdfs_raw_user_metrics"],
        to_dataset=DATASETS["hive_raw_user_metrics"],
        type=DatasetSymlinkTypeDTO.METASTORE,
    ),
]

DATASET_SCHEMAS = {
    "hive_raw_user_metrics": SchemaDTO(
        fields=[
            {"name": "business_dt", "type": "date"},
            {"name": "source", "type": "string"},
            {"name": "id", "type": "integer"},
            {"name": "user", "type": "string"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "value", "type": "float"},
        ],
    ),
    # Spark doesn't know anything about Clickhouse or Postgres column types,
    # This is for demo only.
    "clickhouse_user_metrics": SchemaDTO(
        fields=[
            {"name": "id", "type": "Int64"},
            {"name": "user", "type": "String"},
            {"name": "timestamp", "type": "DateTime"},
            {"name": "business_dt", "type": "Date"},
            {"name": "value", "type": "Float64"},
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


def generate_spark_run_local(
    faker: Faker,
    start: datetime,
    end: datetime,
) -> BatchExtractionResult:
    run_created_at = faker.date_time_between(start, end, tzinfo=UTC)
    run_started_at = run_created_at + timedelta(minutes=faker.pyfloat(min_value=0, max_value=3))
    run_ended_at = run_started_at + timedelta(minutes=faker.pyfloat(min_value=15, max_value=20))
    parent_run = generate_airflow_run(
        "raw_layer_dag",
        "raw_layer_task",
        run_created_at - timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
        run_ended_at + timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
    )

    job = JobDTO(
        name="raw_layer_loader",
        location=LOCATIONS["local"],
        type=JobTypeDTO(type="SPARK_APPLICATION"),
        parent_job=parent_run.job,
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="spark.version"),
                value="4.1.1",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.43.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="environment"),
                value="test",
            ),
        },
    )

    run_id = generate_new_uuid(run_created_at)
    run = RunDTO(
        id=run_id,
        job=job,
        parent_run=parent_run,
        status=RunStatusDTO.SUCCEEDED,
        external_id=f"application_{run_created_at.timestamp() * 1000}_0001",
        running_log_url=f"http://{faker.ipv4_private()}:{faker.port_number(is_user=True)}",
        started_at=run_started_at,
        user=UserDTO(name="raw_user"),
        ended_at=run_ended_at,
    )
    result = BatchExtractionResult()
    result.add_job(job)
    result.add_run(run)

    for symlink in DATASET_SYMLINKS:
        result.add_dataset_symlink(symlink)

    for generator in [clickhouse_to_hive, postgres_to_hive]:
        operation, inputs, outputs, column_lineage = generator(faker, run)
        result.add_operation(operation)
        for input_ in inputs:
            result.add_input(input_)
        for output in outputs:
            result.add_output(output)
        for lineage in column_lineage:
            result.add_column_lineage(lineage)
    return result


def clickhouse_to_hive(
    faker: Faker,
    run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    started_at: datetime = run.started_at  # type: ignore[assignment]
    operation_started_at = started_at + timedelta(seconds=faker.pyfloat(min_value=0, max_value=10))
    operation_ended_at = operation_started_at + timedelta(minutes=faker.pyfloat(min_value=1, max_value=10))
    operation_id = generate_new_uuid(operation_started_at)
    operation = OperationDTO(
        id=operation_id,
        run=run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=operation_started_at,
        ended_at=operation_ended_at,
        name="Clickhouse[clickhouse01] -> Hive",
        position=1,
        sql_query=SQLQueryDTO(
            query=textwrap.dedent(
                """
                SELECT id, user, timestamp, value, CAST(timestamp AS DATE) AS business_dt, 'clickhouse' as source
                FROM user_metrics
                """,
            ).strip(),
        ),
    )
    input_ = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["clickhouse_user_metrics"],
        schema=DATASET_SCHEMAS["clickhouse_user_metrics"],
        # Spark JDBC doesn't report number of ros or bytes
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hdfs_raw_user_metrics"],  # Spark integration quirk
        schema=DATASET_SCHEMAS["hive_raw_user_metrics"],
        type=OutputTypeDTO.CREATE,
        num_bytes=faker.pyint(min_value=2**10, max_value=2**32),
        num_rows=faker.pyint(min_value=10**3, max_value=10**6),
        num_files=faker.pyint(min_value=1, max_value=10**3),
    )
    column_lineage = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input_.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="id",
                target_column="id",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
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
                target_column="business_dt",
                type=DatasetColumnRelationTypeDTO.TRANSFORMATION,
            ),
            DatasetColumnRelationDTO(
                source_column="value",
                target_column="value",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
        ],
    )
    return operation, [input_], [output], [column_lineage]


def postgres_to_hive(
    faker: Faker,
    run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    started_at: datetime = run.started_at  # type: ignore[assignment]
    operation_started_at = started_at + timedelta(seconds=10 * 60 + faker.pyfloat(min_value=0, max_value=10))
    operation_ended_at = operation_started_at + timedelta(minutes=faker.pyfloat(min_value=1, max_value=5))
    operation_id = generate_new_uuid(operation_started_at)
    operation = OperationDTO(
        id=operation_id,
        run=run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=operation_started_at,
        ended_at=operation_ended_at,
        name="Postgres[postgres01] -> Hive",
        position=2,
        sql_query=SQLQueryDTO(
            query=textwrap.dedent(
                """
                SELECT id, user, timestamp, value, timestamp::date AS business_dt, 'postres' as source
                FROM user_metrics
                """,
            ).strip(),
        ),
    )
    input_ = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["postgres_user_metrics"],
        schema=DATASET_SCHEMAS["postgres_user_metrics"],
        # Spark JDBC doesn't report number of ros or bytes
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hdfs_raw_user_metrics"],  # Spark integration quirk
        schema=DATASET_SCHEMAS["hive_raw_user_metrics"],
        type=OutputTypeDTO.APPEND,
        num_bytes=faker.pyint(min_value=2**10, max_value=2**32),
        num_rows=faker.pyint(min_value=10**3, max_value=10**6),
        num_files=faker.pyint(min_value=1, max_value=10**3),
    )
    column_lineage = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input_.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="id",
                target_column="id",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
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
                target_column="business_dt",
                type=DatasetColumnRelationTypeDTO.TRANSFORMATION,
            ),
            DatasetColumnRelationDTO(
                source_column="value",
                target_column="value",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
        ],
    )
    return operation, [input_], [output], [column_lineage]
