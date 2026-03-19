# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from faker import Faker

from data_rentgen.consumer.extractors import BatchExtractionResult
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
    "file": LocationDTO(
        type="file",
        name="host01.companyname.com",
        addresses={"file://host01.companyname.com"},
    ),
    "hive": LocationDTO(
        type="hive",
        name="hadoop.companyname.com",
        addresses={
            "hive://hive01.hadoop.companyname.com:10000",
            "hive://hive01.hadoop.companyname.com:9083",
            "hive://hive02.hadoop.companyname.com:10000",
            "hive://hive02.hadoop.companyname.com:9083",
        },
    ),
    "hdfs": LocationDTO(
        type="hdfs",
        name="hadoop.companyname.com",
        addresses={
            "hdfs://mn01.hadoop.companyname.com:8020",
            "hdfs://mn02.hadoop.companyname.com:8020",
        },
    ),
}

DATASETS = {
    "file_sandbox_raw_user_info": DatasetDTO(
        name="/app/sandbox/user_info.csv",
        location=LOCATIONS["file"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="bronze",
            ),
        },
    ),
    "hive_ref_user_info": DatasetDTO(
        name="ref.user_info",
        location=LOCATIONS["hive"],
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="storage.layer"),
                value="silver",
            ),
        },
    ),
    "hdfs_ref_user_info": DatasetDTO(
        name="/user/hive/warehouse/ref.db/user_info",
        location=LOCATIONS["hdfs"],
    ),
}

DATASET_SYMLINKS = [
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
]

DATASET_SCHEMA = SchemaDTO(
    fields=[
        {"name": "username", "type": "string"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "email", "type": "string"},
    ],
)


def generate_hive_run(
    faker: Faker,
    start: datetime,
    end: datetime,
) -> BatchExtractionResult:
    job = JobDTO(
        name="ref_user@10.176.145.86",
        location=LOCATIONS["hive"],
        type=JobTypeDTO(type="HIVE_SESSION"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="hive.version"),
                value="3.1.2",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.43.0",
            ),
            # no custom tags support for now
            # https://github.com/OpenLineage/OpenLineage/issues/4280
        },
    )

    run_created_at = faker.date_time_between(start, end, tzinfo=UTC)
    run_started_at = run_created_at + timedelta(minutes=faker.pyfloat(min_value=0, max_value=3))
    run_id = generate_new_uuid(run_created_at)
    run = RunDTO(
        id=run_id,
        job=job,
        status=RunStatusDTO.STARTED,  # Hive integration doesn't send finished event
        external_id=str(uuid4()),
        started_at=run_started_at,
        user=UserDTO(name="ref_user"),
    )
    result = BatchExtractionResult()
    result.add_job(job)
    result.add_run(run)

    for symlink in DATASET_SYMLINKS:
        result.add_dataset_symlink(symlink)

    for generator in [load_ref_user_info]:
        operation, inputs, outputs, column_lineage = generator(faker, run)
        result.add_operation(operation)
        for input_ in inputs:
            result.add_input(input_)
        for output in outputs:
            result.add_output(output)
        for lineage in column_lineage:
            result.add_column_lineage(lineage)
    return result


def load_ref_user_info(
    faker: Faker,
    parent_run: RunDTO,
) -> tuple[OperationDTO, list[InputDTO], list[OutputDTO], list[ColumnLineageDTO]]:
    started_at: datetime = parent_run.started_at  # type: ignore[assignment]
    operation_started_at = started_at + timedelta(seconds=faker.pyfloat(min_value=0, max_value=5))
    operation_ended_at = operation_started_at + timedelta(minutes=faker.pyfloat(min_value=5, max_value=10))
    operation_id = generate_new_uuid(operation_started_at)
    operation = OperationDTO(
        id=operation_id,
        run=parent_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        # Hie integration doesn't send started_at for now
        ended_at=operation_ended_at,
        name="ref_user_info_" + operation_started_at.strftime("%Y%m%d%H%M%S") + "_" + str(uuid4()),
        description="LOAD",
        sql_query=SQLQueryDTO(
            # currently Hive integration doesn't support that, but for demo it's ok
            query=textwrap.dedent(
                """
                LOAD DATA LOCAL INPATH '/app/sandbox/user_info.csv'
                OVERWRITE INTO TABLE ref.user_info
                """,
            ).strip(),
        ),
    )
    input_ = InputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["file_sandbox_raw_user_info"],
        schema=DATASET_SCHEMA,
        # currently Hive integration doesn't send IO statistics
    )
    output = OutputDTO(
        created_at=operation.created_at,
        operation=operation,
        dataset=DATASETS["hive_ref_user_info"],
        schema=DATASET_SCHEMA,
        type=OutputTypeDTO.OVERWRITE,
        # currently Hive integration doesn't send IO statistics
    )
    column_lineage = ColumnLineageDTO(
        created_at=operation.created_at,
        operation=operation,
        source_dataset=input_.dataset,
        target_dataset=output.dataset,
        dataset_column_relations=[
            DatasetColumnRelationDTO(
                source_column="username",
                target_column="username",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="first_name",
                target_column="first_name",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="last_name",
                target_column="last_name",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
            DatasetColumnRelationDTO(
                source_column="email",
                target_column="email",
                type=DatasetColumnRelationTypeDTO.IDENTITY,
            ),
        ],
    )
    return operation, [input_], [output], [column_lineage]
