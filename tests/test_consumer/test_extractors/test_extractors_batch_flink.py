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
    SchemaDTO,
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
def test_extractors_extract_batch_flink(
    flink_job_run_event_start: OpenLineageRunEvent,
    flink_job_run_event_stop: OpenLineageRunEvent,
    postgres_input: OpenLineageInputDataset,
    kafka_output: OpenLineageOutputDataset,
    kafka_output_with_stats: OpenLineageOutputDataset,
    extracted_flink_location: LocationDTO,
    extracted_postgres_location: LocationDTO,
    extracted_kafka_location: LocationDTO,
    extracted_postgres_dataset: DatasetDTO,
    extracted_kafka_dataset: DatasetDTO,
    extracted_dataset_schema: SchemaDTO,
    extracted_flink_job: JobDTO,
    extracted_flink_job_run: RunDTO,
    extracted_flink_job_operation: OperationDTO,
    extracted_flink_postgres_input: InputDTO,
    extracted_flink_kafka_output: OutputDTO,
    input_transformation,
):
    events = [
        flink_job_run_event_start.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [kafka_output],
            },
        ),
        flink_job_run_event_stop.model_copy(
            update={
                "inputs": [postgres_input],
                "outputs": [kafka_output_with_stats],
            },
        ),
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_flink_location,
        extracted_kafka_location,
        extracted_postgres_location,
    ]

    assert extracted.jobs() == [extracted_flink_job]
    assert not extracted.users()
    assert extracted.runs() == [extracted_flink_job_run]
    assert extracted.operations() == [extracted_flink_job_operation]

    assert extracted.datasets() == [
        extracted_kafka_dataset,
        extracted_postgres_dataset,
    ]

    assert not extracted.dataset_symlink_groups()

    # Both input & output schemas are the same
    assert extracted.schemas() == [extracted_dataset_schema]
    assert extracted.inputs() == [extracted_flink_postgres_input]
    assert extracted.outputs() == [extracted_flink_kafka_output]
