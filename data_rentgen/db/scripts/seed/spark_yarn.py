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
    "yarn": LocationDTO(
        type="yarn",
        name="hadoop.companyname.com",
        addresses={
            "yarn://mn01.hadoop.companyname.com",
            "yarn://mn02.hadoop.companyname.com",
        },
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
        },
    ),
    "hdfs_raw_user_metrics": DatasetDTO(
        name="/user/hive/warehouse/raw.db/user_metrics",
        location=LOCATIONS["hdfs"],
    ),
    "hive_ref_user_info": DatasetDTO(
        name="ref.user_info",
        location=LOCATIONS["hive_metastore"],
    ),
    "hdfs_ref_user_info": DatasetDTO(
        name="/user/hive/warehouse/ref.db/user_info",
        location=LOCATIONS["hdfs"],
    ),
    "hive_mart_user_metrics_agg": DatasetDTO(
        name="mart.user_metrics_agg",
        location=LOCATIONS["hive_metastore"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="gold",
            ),
            TagValueDTO(
                tag=TagDTO(name="owner.team"),
                value="DE Team 2",
            ),
        },
    ),
    "hdfs_mart_user_metrics_agg": DatasetDTO(
        name="/user/hive/warehouse/mart.db/user_metrics_agg",
        location=LOCATIONS["hdfs"],
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
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hive_ref_user_info"],
        to_dataset=DATASETS["hdfs_ref_user_info"],
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    ),
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hdfs_ref_user_info"],
        to_dataset=DATASETS["hive_ref_user_info"],
        type=DatasetSymlinkTypeDTO.METASTORE,
    ),
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hive_mart_user_metrics_agg"],
        to_dataset=DATASETS["hdfs_mart_user_metrics_agg"],
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    ),
    DatasetSymlinkDTO(
        from_dataset=DATASETS["hdfs_mart_user_metrics_agg"],
        to_dataset=DATASETS["hive_mart_user_metrics_agg"],
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
    "hive_ref_user_info": SchemaDTO(
        fields=[
            {"name": "username", "type": "string"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "email", "type": "string"},
        ],
    ),
    "hive_mart_user_metrics_agg": SchemaDTO(
        fields=[
            {"name": "business_dt", "type": "date"},
            {"name": "source", "type": "string"},
            {"name": "user", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "value", "type": "double"},
        ],
    ),
}


def generate_spark_run_yarn(
    faker: Faker,
    start: datetime,
    end: datetime,
) -> BatchExtractionResult:
    run_created_at = faker.date_time_between(start, end, tzinfo=UTC)
    run_started_at = run_created_at + timedelta(minutes=faker.pyfloat(min_value=0, max_value=3))
    run_ended_at = run_started_at + timedelta(minutes=faker.pyfloat(min_value=10, max_value=12))
    paren_run = generate_airflow_run(
        "mart_layer_dag",
        "mart_layer_task",
        run_created_at - timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
        run_ended_at + timedelta(seconds=faker.pyint(min_value=5, max_value=10)),
    )

    job = JobDTO(
        name="mart_layer_loader",
        location=LOCATIONS["yarn"],
        type=JobTypeDTO(type="SPARK_APPLICATION"),
        parent_job=paren_run.job,
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="spark.version"),
                value="3.5.7",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.38.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="environment"),
                value="production",
            ),
        },
    )

    run_id = generate_new_uuid(run_created_at)
    external_id = f"application_{run_created_at.timestamp() * 1000}_0001"
    run = RunDTO(
        id=run_id,
        job=job,
        status=RunStatusDTO.SUCCEEDED,
        parent_run=paren_run,
        external_id=external_id,
        running_log_url=f"http://{faker.ipv4_private()}:{faker.port_number(is_user=True)}",
        persistent_log_url=f"http://mn01.hadoop.companyname.com:8088/proxy/{external_id}",
        started_at=run_started_at,
        user=UserDTO(name="mart_user"),
        ended_at=run_ended_at,
    )
    result = BatchExtractionResult()
    result.add_job(job)
    result.add_run(run)

    for symlink in DATASET_SYMLINKS:
        result.add_dataset_symlink(symlink)

    for generator in [raw_to_mart]:
        operation, inputs, outputs, column_lineage = generator(faker, run)
        result.add_operation(operation)
        for input_ in inputs:
            result.add_input(input_)
        for output in outputs:
            result.add_output(output)
        for lineage in column_lineage:
            result.add_column_lineage(lineage)
    return result


def raw_to_mart(
    faker: Faker,
    run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    started_at: datetime = run.started_at  # type: ignore[assignment]
    operation_started_at = started_at + timedelta(seconds=faker.pyfloat(min_value=0, max_value=5))
    operation_ended_at = operation_started_at + timedelta(minutes=faker.pyfloat(min_value=5, max_value=10))
    operation_id = generate_new_uuid(operation_started_at)
    operation = OperationDTO(
        id=operation_id,
        run=run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=operation_started_at,
        ended_at=operation_ended_at,
        name="RAW -> MART",
        position=1,
        sql_query=SQLQueryDTO(
            query=textwrap.dedent(
                """
                WITH raw_data AS (
                    SELECT
                        id,
                        user,
                        timestamp,
                        business_dt,
                        value,
                        source
                    FROM raw.user_metrics
                    WHERE business_dt = '{operation_started_at.date()}'
                )
                SELECT
                    raw_data.business_dt,
                    raw_data.source,
                    raw_data.user,
                    user.email,
                    MAX(raw_data.timestamp) as timestamp,
                    AVG(raw_data.value) as value
                FROM raw_data AS raw
                JOIN ref.user_info AS users ON raw_data.user = users.username
                WHERE users.username != 'unknown'
                GROUP BY raw_data.user
                ORDER BY raw_data.business_dt, raw_data.source, timestamp
                """,
            ).strip(),
        ),
    )
    input1 = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hive_raw_user_metrics"],
        schema=DATASET_SCHEMAS["hive_raw_user_metrics"],
        num_bytes=faker.pyint(min_value=2**10, max_value=2**32),
        num_rows=faker.pyint(min_value=10**3, max_value=10**6),
        num_files=faker.pyint(min_value=1, max_value=10**3),
    )
    input2 = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hive_ref_user_info"],
        schema=DATASET_SCHEMAS["hive_ref_user_info"],
        num_bytes=faker.pyint(min_value=2**10, max_value=2**32),
        num_rows=faker.pyint(min_value=10**3, max_value=10**6),
        num_files=faker.pyint(min_value=1, max_value=10**3),
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hdfs_mart_user_metrics_agg"],  # Spark integration quirk
        schema=DATASET_SCHEMAS["hive_mart_user_metrics_agg"],  # Spark integration quirk
        type=OutputTypeDTO.CREATE,
        num_bytes=input1.num_bytes + input2.num_bytes,  # type: ignore[operator]
        num_rows=input1.num_rows + input2.num_rows,  # type: ignore[operator]
        num_files=max(input1.num_files, input2.num_files) + 1,  # type: ignore[type-var, operator]
    )
    column_lineage1 = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input1.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="business_dt",
                target_column="business_dt",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="source",
                target_column="source",
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
                type=DatasetColumnRelationTypeDTO.AGGREGATION,
            ),
            DatasetColumnRelationDTO(
                source_column="value",
                target_column="value",
                type=DatasetColumnRelationTypeDTO.AGGREGATION,
            ),
            DatasetColumnRelationDTO(
                source_column="user",
                type=(
                    DatasetColumnRelationTypeDTO.FILTER
                    | DatasetColumnRelationTypeDTO.GROUP_BY
                    | DatasetColumnRelationTypeDTO.JOIN
                ),
            ),
            DatasetColumnRelationDTO(
                source_column="business_dt",
                type=DatasetColumnRelationTypeDTO.GROUP_BY | DatasetColumnRelationTypeDTO.SORT,
            ),
            DatasetColumnRelationDTO(
                source_column="source",
                type=DatasetColumnRelationTypeDTO.GROUP_BY | DatasetColumnRelationTypeDTO.SORT,
            ),
            DatasetColumnRelationDTO(
                source_column="timestamp",
                type=DatasetColumnRelationTypeDTO.SORT,
            ),
        ],
    )
    column_lineage2 = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input2.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="username",
                target_column="user",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="email",
                target_column="email",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="username",
                type=DatasetColumnRelationTypeDTO.JOIN,
            ),
        ],
    )
    return operation, [input1, input2], [output], [column_lineage1, column_lineage2]
