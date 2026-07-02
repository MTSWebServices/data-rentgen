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
    SQLQueryDTO,
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
def test_extractors_extract_batch_dbt_spark_thrift(
    dbt_job_run_event_start: OpenLineageRunEvent,
    dbt_job_run_event_stop: OpenLineageRunEvent,
    dbt_model_run_event_start: OpenLineageRunEvent,
    dbt_model_run_event_stop: OpenLineageRunEvent,
    extracted_spark_thrift_location: LocationDTO,
    extracted_dbt_location: LocationDTO,
    extracted_dbt_spark_source_dataset: DatasetDTO,
    extracted_dbt_spark_target_dataset: DatasetDTO,
    extracted_dbt_job: JobDTO,
    extracted_dbt_run: RunDTO,
    extracted_dbt_operation: OperationDTO,
    extracted_dbt_spark_source_schema: SchemaDTO,
    extracted_dbt_sql_query: SQLQueryDTO,
    extracted_dbt_spark_input: InputDTO,
    extracted_dbt_spark_output: OutputDTO,
    input_transformation,
):
    events = [
        dbt_job_run_event_start,
        dbt_model_run_event_start,
        dbt_model_run_event_stop,
        dbt_job_run_event_stop,
    ]

    extracted = BatchExtractor().add_events(input_transformation(events))

    assert extracted.locations() == [
        extracted_dbt_location,
        extracted_spark_thrift_location,
    ]

    assert extracted.jobs() == [extracted_dbt_job]
    assert extracted.users() == []
    assert extracted.runs() == [extracted_dbt_run]
    assert extracted.operations() == [extracted_dbt_operation]
    assert extracted.sql_queries() == [extracted_dbt_sql_query]

    assert extracted.datasets() == [
        extracted_dbt_spark_source_dataset,
        extracted_dbt_spark_target_dataset,
    ]

    assert not extracted.dataset_symlink_groups()

    assert extracted.schemas() == [extracted_dbt_spark_source_schema]
    assert extracted.inputs() == [extracted_dbt_spark_input]
    assert extracted.outputs() == [extracted_dbt_spark_output]
