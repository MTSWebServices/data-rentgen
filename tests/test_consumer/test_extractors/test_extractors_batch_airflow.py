import pytest

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
from data_rentgen.openlineage.dataset import OpenLineageInputDataset, OpenLineageOutputDataset
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
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
def test_extractors_extract_batch_airflow_without_lineage(
    airflow_dag_run_event_start: OpenLineageRunEvent,
    airflow_dag_run_event_stop: OpenLineageRunEvent,
    airflow_task1_run_event_start: OpenLineageRunEvent,
    airflow_task1_run_event_stop: OpenLineageRunEvent,
    extracted_airflow_location: LocationDTO,
    extracted_airflow_dag_job: JobDTO,
    extracted_airflow_task1_job: JobDTO,
    extracted_airflow_dag_run: RunDTO,
    extracted_airflow_task1_run: RunDTO,
    extracted_airflow_task1_operation: OperationDTO,
    input_transformation,
):
    events = [
        airflow_dag_run_event_start,
        airflow_task1_run_event_start,
        airflow_task1_run_event_stop,
        airflow_dag_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [extracted_airflow_location]
    assert extracted.jobs() == [extracted_airflow_dag_job, extracted_airflow_task1_job]
    assert extracted.runs() == [
        extracted_airflow_dag_run,
        extracted_airflow_task1_run,
    ]
    assert extracted.operations() == [extracted_airflow_task1_operation]

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
def test_extractors_extract_batch_airflow_with_lineage(
    airflow_dag_run_event_start: OpenLineageRunEvent,
    airflow_dag_run_event_stop: OpenLineageRunEvent,
    airflow_task1_run_event_start: OpenLineageRunEvent,
    airflow_task1_run_event_stop: OpenLineageRunEvent,
    postgres_input: OpenLineageInputDataset,
    hdfs_output: OpenLineageOutputDataset,
    hdfs_output_with_stats: OpenLineageOutputDataset,
    extracted_airflow_dag_job: JobDTO,
    extracted_airflow_task1_job: JobDTO,
    extracted_airflow_dag_run: RunDTO,
    extracted_airflow_task1_run: RunDTO,
    extracted_airflow_task1_operation: OperationDTO,
    extracted_airflow_location: LocationDTO,
    extracted_postgres_location: LocationDTO,
    extracted_hdfs_location: LocationDTO,
    extracted_hive_metastore_location: LocationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_hive_dataset1: DatasetDTO,
    extracted_dataset1_symlink_group: DatasetSymlinkGroupDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_user: UserDTO,
    extracted_airflow_postgres_input: InputDTO,
    extracted_airflow_hdfs_output: OutputDTO,
    input_transformation,
):
    events = [
        airflow_dag_run_event_start,
        airflow_task1_run_event_start.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output],
            },
        ),
        airflow_task1_run_event_stop.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [hdfs_output_with_stats],
            },
        ),
        airflow_dag_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_hdfs_location,
        extracted_hive_metastore_location,
        extracted_airflow_location,
        extracted_postgres_location,
    ]

    assert extracted.jobs() == [extracted_airflow_dag_job, extracted_airflow_task1_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_airflow_dag_run, extracted_airflow_task1_run]
    assert extracted.operations() == [extracted_airflow_task1_operation]

    assert extracted.datasets() == [
        extracted_hdfs_dataset1,
        extracted_hive_dataset1,
        extracted_postgres_dataset,
    ]

    assert extracted.dataset_symlink_groups() == [extracted_dataset1_symlink_group]

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_airflow_postgres_input]
    assert extracted.outputs() == [extracted_airflow_hdfs_output]
