import pytest

from data_rentgen.consumer.extractors import BatchExtractor
from data_rentgen.dto import (
    DatasetDTO,
    InputDTO,
    JobDTO,
    LocationDTO,
    OperationDTO,
    OutputDTO,
    RunDTO,
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
def test_extractors_extract_batch_starrocks(
    starrocks_query_run_event_start: OpenLineageRunEvent,
    starrocks_query_run_event_stop: OpenLineageRunEvent,
    iceberg_input: OpenLineageInputDataset,
    iceberg_output: OpenLineageOutputDataset,
    extracted_iceberg_metastore_location: LocationDTO,
    extracted_starrocks_location: LocationDTO,
    extracted_iceberg_dataset1: DatasetDTO,
    extracted_iceberg_dataset2: DatasetDTO,
    extracted_starrocks_job: JobDTO,
    extracted_starrocks_run: RunDTO,
    extracted_starrocks_operation: OperationDTO,
    extracted_starrocks_input: InputDTO,
    extracted_starrocks_output: OutputDTO,
    extracted_user: UserDTO,
    input_transformation,
):
    events = [
        starrocks_query_run_event_start,
        starrocks_query_run_event_stop.model_copy(
            update={
                "inputs": [iceberg_input],
                "outputs": [iceberg_output],
            },
        ),
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_iceberg_metastore_location,
        extracted_starrocks_location,
    ]

    assert extracted.jobs() == [extracted_starrocks_job]
    assert extracted.users() == [extracted_user]
    assert extracted.runs() == [extracted_starrocks_run]
    assert extracted.operations() == [extracted_starrocks_operation]

    assert extracted.datasets() == [
        extracted_iceberg_dataset1,
        extracted_iceberg_dataset2,
    ]

    assert not extracted.dataset_symlinks()
    assert not extracted.schemas()

    assert extracted.inputs() == [extracted_starrocks_input]
    assert extracted.outputs() == [extracted_starrocks_output]
