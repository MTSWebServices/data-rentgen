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
    UserDTO,
)


@pytest.fixture
def extracted_spark_location() -> LocationDTO:
    return LocationDTO(
        type="local",
        name="some.host.com",
        addresses={"local://some.host.com"},
    )


@pytest.fixture
def extracted_spark_app_job(
    extracted_spark_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession1",
        location=extracted_spark_location,
        type=JobTypeDTO(type="SPARK_APPLICATION"),
    )


@pytest.fixture
def extracted_spark_app_job_with_parent(
    extracted_spark_location: LocationDTO,
    extracted_airflow_task1_job_as_parent: JobDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession1",
        location=extracted_spark_location,
        type=JobTypeDTO(type="SPARK_APPLICATION"),
        parent_job=extracted_airflow_task1_job_as_parent,
    )


@pytest.fixture
def extracted_spark_another_app_job(
    extracted_spark_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession2",
        location=extracted_spark_location,
        type=JobTypeDTO(type="SPARK_APPLICATION"),
    )


@pytest.fixture
def extracted_spark_another_app_job_with_parent(
    extracted_spark_location: LocationDTO,
    extracted_airflow_task2_job: JobDTO,
) -> JobDTO:
    return JobDTO(
        name="mysession2",
        location=extracted_spark_location,
        type=JobTypeDTO(type="SPARK_APPLICATION"),
        parent_job=extracted_airflow_task2_job,
    )


@pytest.fixture
def extracted_spark_unknown_job(
    extracted_spark_location: LocationDTO,
) -> JobDTO:
    return JobDTO(
        name="unknown",
        location=extracted_spark_location,
        type=JobTypeDTO(type="SPARK_APPLICATION"),
    )


@pytest.fixture
def extracted_spark_app_run(
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        job=extracted_spark_app_job,
        user=extracted_user,
        status=RunStatusDTO.SUCCEEDED,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
        external_id="local-1719136537510",
        running_log_url="http://127.0.0.1:4040",
    )


@pytest.fixture
def extracted_spark_app_run_with_parent(
    extracted_spark_app_job_with_parent: JobDTO,
    extracted_airflow_task1_run_as_parent: RunDTO,
    extracted_user: UserDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
        job=extracted_spark_app_job_with_parent,
        user=extracted_user,
        status=RunStatusDTO.SUCCEEDED,
        parent_run=extracted_airflow_task1_run_as_parent,
        started_at=datetime(2024, 7, 5, 9, 4, 48, 794900, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 646000, tzinfo=timezone.utc),
        external_id="local-1719136537510",
        running_log_url="http://127.0.0.1:4040",
    )


@pytest.fixture
def extracted_spark_another_app_run_with_parent(
    extracted_spark_another_app_job_with_parent: JobDTO,
    extracted_airflow_task2_run: RunDTO,
) -> RunDTO:
    return RunDTO(
        id=UUID("01908222-84bb-7fdf-ac0b-36892235c3db"),
        job=extracted_spark_another_app_job_with_parent,
        status=RunStatusDTO.SUCCEEDED,
        parent_run=extracted_airflow_task2_run,
        started_at=datetime(2024, 7, 5, 9, 3, 38, 683800, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 6, 15, 646000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_spark_operation(
    extracted_spark_app_run: RunDTO,
):
    return OperationDTO(
        id=UUID("01908225-1fd7-746b-910c-70d24f2898b1"),
        name="execute_some_command",
        run=extracted_spark_app_run,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_spark_operation_with_parent(
    extracted_spark_app_run_with_parent: RunDTO,
):
    return OperationDTO(
        id=UUID("01908225-1fd7-746b-910c-70d24f2898b1"),
        name="execute_some_command",
        run=extracted_spark_app_run_with_parent,
        status=OperationStatusDTO.SUCCEEDED,
        type=OperationTypeDTO.BATCH,
        started_at=datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc),
        ended_at=datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc),
    )


@pytest.fixture
def extracted_spark_postgres_input(
    extracted_spark_operation: OperationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> InputDTO:
    return InputDTO(
        created_at=extracted_spark_operation.created_at,
        operation=extracted_spark_operation,
        dataset=extracted_postgres_dataset,
        schema=extracted_dataset_schema,
    )


@pytest.fixture
def extracted_spark_hive_output(
    extracted_spark_operation: OperationDTO,
    extracted_hive_dataset1: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
) -> OutputDTO:
    return OutputDTO(
        created_at=extracted_spark_operation.created_at,
        type=OutputTypeDTO.CREATE,
        operation=extracted_spark_operation,
        dataset=extracted_hive_dataset1,
        schema=extracted_dataset_schema,
        num_rows=1_000_000,
        num_bytes=1000 * 1024 * 1024,
    )


def get_spark_operation_dto(operation_id: UUID) -> OperationDTO:
    return OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=UUID("01908224-8410-79a2-8de6-a769ad6944c9"),
            job=JobDTO(
                name="mysession1",
                location=LocationDTO(
                    type="local",
                    name="some.host.com",
                    addresses={
                        "local://some.host.com",
                    },
                    id=None,
                ),
                type=None,
                id=None,
            ),
            parent_run=None,
            status=RunStatusDTO.UNKNOWN,
            started_at=None,
            start_reason=None,
            user=None,
            ended_at=None,
            external_id=None,
            attempt=None,
            running_log_url=None,
            persistent_log_url=None,
        ),
        name="execute_some_command",
        type=OperationTypeDTO.BATCH,
        position=None,
        group=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=None,
        ended_at=None,
    )
