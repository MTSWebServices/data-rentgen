from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors import BatchExtractor
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkGroupDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
    SchemaDTO,
    UserDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)


@pytest.mark.parametrize(
    "input_transformation",
    [
        # receiving data out of order does not change result
        pytest.param(
            list,
            id="preserve order",
        ),
        pytest.param(
            reversed,
            id="reverse order",
        ),
    ],
)
def test_extractors_extract_batch_spark_without_lineage(
    spark_app_run_event_start: OpenLineageRunEvent,
    spark_app_run_event_stop: OpenLineageRunEvent,
    spark_operation_run_event_start: OpenLineageRunEvent,
    spark_operation_run_event_running: OpenLineageRunEvent,
    spark_operation_run_event_stop: OpenLineageRunEvent,
    extracted_spark_location: LocationDTO,
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
    extracted_spark_app_run: RunDTO,
    extracted_spark_operation: OperationDTO,
    input_transformation,
):
    events = [
        spark_app_run_event_start,
        spark_operation_run_event_start,
        spark_operation_run_event_running,
        spark_operation_run_event_stop,
        spark_app_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_spark_location,
    ]

    assert extracted.jobs() == [extracted_spark_app_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_spark_app_run]
    assert extracted.operations() == [extracted_spark_operation]

    assert not extracted.datasets()
    assert not extracted.dataset_symlink_groups()
    assert not extracted.schemas()
    assert not extracted.inputs()
    assert not extracted.outputs()


def test_extractors_extract_batch_spark_openlineage_emitted_unknown_name(
    spark_app_run_event_start_with_unknown_name: OpenLineageRunEvent,
    spark_app_run_event_stop: OpenLineageRunEvent,
    spark_operation_run_event_start: OpenLineageRunEvent,
    spark_operation_run_event_running: OpenLineageRunEvent,
    spark_operation_run_event_stop: OpenLineageRunEvent,
    extracted_spark_location: LocationDTO,
    extracted_spark_app_job: JobDTO,
    extracted_spark_unknown_job: JobDTO,
    extracted_user: UserDTO,
    extracted_spark_app_run: RunDTO,
    extracted_spark_operation: OperationDTO,
):
    events = [
        spark_app_run_event_start_with_unknown_name,
        spark_operation_run_event_start,
        spark_operation_run_event_running,
        spark_operation_run_event_stop,
        spark_app_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(events)

    # the result is the same as above
    assert extracted.locations() == [
        extracted_spark_location,
    ]

    assert extracted.jobs() == [extracted_spark_app_job, extracted_spark_unknown_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_spark_app_run]
    assert extracted.operations() == [extracted_spark_operation]

    assert not extracted.datasets()
    assert not extracted.dataset_symlink_groups()
    assert not extracted.schemas()
    assert not extracted.inputs()
    assert not extracted.outputs()


def test_extractors_extract_batch_spark_openlineage_emitted_unknown_name_no_job_mixing(
    spark_another_app_run_event_start_with_unknown_name_and_different_parent: OpenLineageRunEvent,
    spark_app_run_event_start_with_unknown_name_and_parent: OpenLineageRunEvent,
    spark_app_run_event_stop_with_parent: OpenLineageRunEvent,
    spark_another_app_run_event_stop_with_another_parent: OpenLineageRunEvent,
    spark_operation_run_event_start: OpenLineageRunEvent,
    spark_operation_run_event_running: OpenLineageRunEvent,
    spark_operation_run_event_stop: OpenLineageRunEvent,
    extracted_airflow_location: LocationDTO,
    extracted_airflow_task1_job_as_parent: JobDTO,
    extracted_airflow_task1_run_as_parent: RunDTO,
    extracted_another_airflow_location: LocationDTO,
    extracted_airflow_task2_job: JobDTO,
    extracted_airflow_task2_run: RunDTO,
    extracted_spark_location: LocationDTO,
    extracted_spark_app_job_with_parent: JobDTO,
    extracted_spark_another_app_job_with_parent: JobDTO,
    extracted_spark_unknown_job: JobDTO,
    extracted_user: UserDTO,
    extracted_spark_app_run_with_parent: RunDTO,
    extracted_spark_another_app_run_with_parent: RunDTO,
    extracted_spark_operation_with_parent: OperationDTO,
):
    events = [
        spark_another_app_run_event_start_with_unknown_name_and_different_parent,
        spark_app_run_event_start_with_unknown_name_and_parent,
        spark_operation_run_event_start,
        spark_operation_run_event_running,
        spark_operation_run_event_stop,
        spark_another_app_run_event_stop_with_another_parent,
        spark_app_run_event_stop_with_parent,
    ]

    extracted = BatchExtractor().add_events(events)

    extracted_spark_unknown_job.parent_job = extracted_airflow_task1_job_as_parent

    # the result is the same as above
    assert extracted.locations() == [
        extracted_airflow_location,
        extracted_another_airflow_location,
        extracted_spark_location,
    ]

    assert extracted.jobs() == [
        extracted_airflow_task1_job_as_parent,
        extracted_airflow_task2_job,
        extracted_spark_app_job_with_parent,
        extracted_spark_another_app_job_with_parent,
        extracted_spark_unknown_job,
    ]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [
        extracted_airflow_task2_run,
        extracted_spark_another_app_run_with_parent,
        extracted_airflow_task1_run_as_parent,
        extracted_spark_app_run_with_parent,
    ]
    assert extracted.operations() == [extracted_spark_operation_with_parent]

    assert not extracted.datasets()
    assert not extracted.dataset_symlink_groups()
    assert not extracted.schemas()
    assert not extracted.inputs()
    assert not extracted.outputs()


@pytest.mark.parametrize(
    "input_transformation",
    [
        # receiving data out of order does not change result
        pytest.param(
            list,
            id="preserve order",
        ),
        pytest.param(
            reversed,
            id="reverse order",
        ),
    ],
)
def test_extractors_extract_batch_spark_with_lineage(
    spark_app_run_event_start: OpenLineageRunEvent,
    spark_app_run_event_stop: OpenLineageRunEvent,
    spark_operation_run_event_start: OpenLineageRunEvent,
    spark_operation_run_event_running: OpenLineageRunEvent,
    spark_operation_run_event_stop: OpenLineageRunEvent,
    postgres_input: OpenLineageInputDataset,
    hdfs_output: OpenLineageOutputDataset,
    hdfs_output_with_stats: OpenLineageOutputDataset,
    extracted_postgres_location: LocationDTO,
    extracted_hdfs_location: LocationDTO,
    extracted_hive_metastore_location: LocationDTO,
    extracted_spark_location: LocationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_hive_dataset1: DatasetDTO,
    extracted_dataset1_symlink_group: DatasetSymlinkGroupDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_spark_app_job: JobDTO,
    extracted_user: UserDTO,
    extracted_spark_app_run: RunDTO,
    extracted_spark_operation: OperationDTO,
    extracted_spark_postgres_input: InputDTO,
    extracted_spark_hive_output: OutputDTO,
    input_transformation,
):
    events = [
        spark_app_run_event_start,
        spark_operation_run_event_start.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output],
            },
        ),
        spark_operation_run_event_running.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output_with_stats],
            },
        ),
        spark_operation_run_event_stop.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output_with_stats],
            },
        ),
        spark_app_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_hdfs_location,
        extracted_hive_metastore_location,
        extracted_spark_location,
        extracted_postgres_location,
    ]

    assert extracted.jobs() == [extracted_spark_app_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_spark_app_run]
    assert extracted.operations() == [extracted_spark_operation]

    assert extracted.datasets() == [
        extracted_hdfs_dataset1,
        extracted_hive_dataset1,
        extracted_postgres_dataset,
    ]

    assert extracted.dataset_symlink_groups() == [extracted_dataset1_symlink_group]

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_spark_postgres_input]
    assert extracted.outputs() == [extracted_spark_hive_output]


def test_extractors_extract_batch_spark_strip_hdfs_partitions(extracted_hdfs_dataset1: DatasetDTO):
    """
    There is two datasets name in event. They should be union into one, excluding partition.
    """
    event_time = datetime(2024, 7, 5, 9, 7, 9, 849000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation_id = UUID("01908225-1fd7-746b-910c-70d24f2898b1")
    event = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
            name="mysession1.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="local://some.host.com",
                        name="mysession1",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        outputs=[
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable1/business_dt=2025-01-01/reg_id=99/part_dt=2025-01-01",
            ),
            OpenLineageOutputDataset(
                namespace="hdfs://test-hadoop:9820",
                name="/user/hive/warehouse/mydb.db/mytable1/business_dt=2025-02-01/reg_id=99/part_dt=2025-01-01",
            ),
        ],
    )

    extracted = BatchExtractor().add_events([event])
    assert extracted.datasets() == [extracted_hdfs_dataset1]
