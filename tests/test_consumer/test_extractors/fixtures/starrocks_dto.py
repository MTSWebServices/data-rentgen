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
    SQLQueryDTO,
    UserDTO,
)


@pytest.fixture
def extracted_starrocks_location() -> LocationDTO:
    return LocationDTO(
        type="starrocks",
        name="some-starrocks:9030",
        addresses={"starrocks://some-starrocks:9030"},
    )


@pytest.fixture
def extracted_starrocks_job(
    extracted_starrocks_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="myuser@11.22.33.44",
        location=extracted_starrocks_location,
        type=JobTypeDTO(type="STARROCKS_SESSION"),
    )


@pytest.fixture
def extracted_starrocks_run(
    extracted_starrocks_job: JobDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
        job=extracted_starrocks_job,
        status=RunStatusDTO.STARTED,
        started_at=datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc),
        start_reason=None,
        user=extracted_user,
        ended_at=None,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


@pytest.fixture
def extracted_starrocks_operation(
    extracted_starrocks_run: RunDTO,
) -> OperationDTO:
    return OperationDTO(
        id=UUID("019d455f-d088-7D61-bdcd-6a9085a2096a"),
        run=extracted_starrocks_run,
        name="insert_query",
        description=None,
        type=OperationTypeDTO.BATCH,
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=datetime(2026, 3, 31, 19, 30, 0, 456123, tzinfo=timezone.utc),
        ended_at=datetime(2026, 3, 31, 19, 40, 0, 456123, tzinfo=timezone.utc),
        sql_query=SQLQueryDTO(query="INSERT INTO users_backup SELECT * FROM users WHERE active = true"),
    )


@pytest.fixture
def extracted_starrocks_input(
    extracted_starrocks_operation: OperationDTO,
    extracted_iceberg_dataset1: DatasetDTO,
) -> InputDTO:
    return InputDTO(
        created_at=extracted_starrocks_operation.created_at,
        operation=extracted_starrocks_operation,
        dataset=extracted_iceberg_dataset1,
        schema=None,
    )


@pytest.fixture
def extracted_starrocks_output(
    extracted_starrocks_operation: OperationDTO,
    extracted_iceberg_dataset2: DatasetDTO,
) -> OutputDTO:
    return OutputDTO(
        created_at=extracted_starrocks_operation.created_at,
        type=OutputTypeDTO.APPEND,
        operation=extracted_starrocks_operation,
        dataset=extracted_iceberg_dataset2,
        schema=None,
    )
