from datetime import datetime, timezone
from uuid import UUID

import pytest

from data_rentgen.dto import (
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
    UserDTO,
)


@pytest.fixture
def extracted_hive_thrift_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:10000",
        addresses={"hive://test-hadoop:10000"},
    )


@pytest.fixture
def extracted_hive_metastore_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:9083",
        addresses={"hive://test-hadoop:9083"},
    )


@pytest.fixture
def extracted_hive_job(
    extracted_hive_thrift_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="myuser@11.22.33.44",
        location=extracted_hive_thrift_location,
        type=JobTypeDTO(type="HIVE_SESSION"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="hive.version"),
                value="3.1.3",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.34.0",
            ),
        },
    )


@pytest.fixture
def extracted_hive_run(
    extracted_hive_job: JobDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("0197833d-511d-7034-be27-078fb128ca59"),
        job=extracted_hive_job,
        status=RunStatusDTO.STARTED,
        started_at=datetime(2025, 6, 18, 13, 32, 3, 229000, tzinfo=timezone.utc),
        start_reason=None,
        user=extracted_user,
        ended_at=None,
        external_id="0ba6765b-3019-4172-b748-63c257158d20",
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


@pytest.fixture
def extracted_hive_operation(
    extracted_hive_run: RunDTO,
) -> OperationDTO:
    return OperationDTO(
        id=UUID("0197833d-6cec-7609-a80f-8f4e0f8a5b1f"),
        run=extracted_hive_run,
        name="hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45",
        description="CREATETABLE_AS_SELECT",
        type=OperationTypeDTO.BATCH,
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=datetime(2025, 6, 18, 13, 32, 10, 30000, tzinfo=timezone.utc),
        sql_query=SQLQueryDTO(query="create table mydatabase.target_table as select * from mydatabase.source_table"),
    )


@pytest.fixture
def extracted_hive_input(
    extracted_hive_operation: OperationDTO,
    extracted_hive_dataset1: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        created_at=extracted_hive_operation.created_at,
        operation=extracted_hive_operation,
        dataset=extracted_hive_dataset1,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_hive_output(
    extracted_hive_operation: OperationDTO,
    extracted_hive_dataset2: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        created_at=extracted_hive_operation.created_at,
        type=OutputTypeDTO.CREATE,
        operation=extracted_hive_operation,
        dataset=extracted_hive_dataset2,
        schema=extracted_dataset_schema,
    )
