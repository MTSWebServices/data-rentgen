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
from data_rentgen.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
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
def test_extractors_extract_batch_hive(
    hive_query_run_event_stop: OpenLineageRunEvent,
    hive_input: OpenLineageInputDataset,
    hive_output: OpenLineageOutputDataset,
    extracted_hive_thrift_location: LocationDTO,
    extracted_hive_metastore_location: LocationDTO,
    extracted_hdfs_location: LocationDTO,
    extracted_hive_dataset1: DatasetDTO,
    extracted_hive_dataset2: DatasetDTO,
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_hdfs_dataset2: DatasetDTO,
    extracted_dataset1_symlink_group: DatasetSymlinkGroupDTO,
    extracted_dataset2_symlink_group: DatasetSymlinkGroupDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_hive_job: JobDTO,
    extracted_hive_run: RunDTO,
    extracted_hive_operation: OperationDTO,
    extracted_hive_input: InputDTO,
    extracted_hive_output: OutputDTO,
    extracted_user: UserDTO,
    input_transformation,
):
    events = [
        hive_query_run_event_stop.model_copy(
            update={
                "inputs": [hive_input],
                "outputs": [hive_output],
            },
        ),
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_hdfs_location,
        extracted_hive_thrift_location,
        extracted_hive_metastore_location,
    ]

    assert extracted.jobs() == [extracted_hive_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_hive_run]
    assert extracted.operations() == [extracted_hive_operation]

    assert extracted.datasets() == [
        extracted_hdfs_dataset1,
        extracted_hdfs_dataset2,
        extracted_hive_dataset1,
        extracted_hive_dataset2,
    ]

    assert [group.fingerprint for group in extracted.dataset_symlink_groups()] == [
        extracted_dataset2_symlink_group.fingerprint,
        extracted_dataset1_symlink_group.fingerprint,
    ]

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_hive_input]
    assert extracted.outputs() == [extracted_hive_output]
