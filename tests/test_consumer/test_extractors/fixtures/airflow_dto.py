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
    RunStartReasonDTO,
    RunStatusDTO,
    SchemaDTO,
    TagDTO,
    TagValueDTO,
    UserDTO,
)


@pytest.fixture
def extracted_airflow_location() -> LocationDTO:
    return LocationDTO(
        type="http",
        name="airflow-host:8081",
        addresses={"http://airflow-host:8081"},
    )


@pytest.fixture
def extracted_airflow_dag_job(
    extracted_airflow_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mydag",
        location=extracted_airflow_location,
        type=JobTypeDTO(type="AIRFLOW_DAG"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="airflow.version"),
                value="2.9.2",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.10.0",
            ),
        },
    )


@pytest.fixture
def extracted_airflow_task_job(
    extracted_airflow_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mydag.mytask",
        location=extracted_airflow_location,
        type=JobTypeDTO(type="AIRFLOW_TASK"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="airflow.version"),
                value="2.9.2",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="1.10.0",
            ),
        },
    )


@pytest.fixture
def extracted_airflow_dag_run(
    extracted_airflow_dag_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908223-0782-79b8-9495-b1c38aaee839"),
        job=extracted_airflow_dag_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        start_reason=RunStartReasonDTO.MANUAL,
        user=UserDTO(
            name="myuser",
            id=None,
        ),
        ended_at=datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc),
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        persistent_log_url="http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00",
    )


@pytest.fixture
def extracted_airflow_task_run(
    extracted_airflow_task_job: JobDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
        job=extracted_airflow_task_job,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        start_reason=RunStartReasonDTO.MANUAL,
        user=UserDTO(
            name="myuser",
            id=None,
        ),
        ended_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
        ),
    )


@pytest.fixture
def extracted_airflow_task_operation(
    extracted_airflow_task_run: RunDTO,
) -> OperationDTO:
    return OperationDTO(
        id=UUID("01908223-0782-7fc0-9d69-b1df9dac2c60"),
        name="mytask",
        description="BashOperator",
        run=extracted_airflow_task_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_airflow_postgres_input(
    extracted_airflow_task_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        created_at=extracted_airflow_task_operation.created_at,
        operation=extracted_airflow_task_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_airflow_hdfs_output(
    extracted_airflow_task_operation: OperationDTO,
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        created_at=extracted_airflow_task_operation.created_at,
        type=OutputTypeDTO.CREATE,
        operation=extracted_airflow_task_operation,
        dataset=extracted_hdfs_dataset1,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
    )
