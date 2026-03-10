import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from faststream.kafka import KafkaBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from uuid6 import UUID

from data_rentgen.db.models import (
    ColumnLineage,
    Dataset,
    DatasetColumnRelation,
    DatasetSymlink,
    Input,
    Job,
    JobDependency,
    Location,
    Operation,
    OperationStatus,
    OperationType,
    Output,
    OutputType,
    Run,
    RunStatus,
    Schema,
    TagValue,
)

RESOURCES_PATH = Path(__file__).parent.parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_flink() -> list[dict]:
    lines = (RESOURCES_PATH / "events_flink.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


@pytest.mark.asyncio
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
async def test_runs_handler_flink(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_flink: list[dict],
    input_transformation,
):
    for event in input_transformation(events_flink):
        await test_broker.publish(event, "input.runs")

    job_query = (
        select(Job)
        .order_by(Job.name)
        .options(
            selectinload(Job.location).selectinload(Location.addresses),
            selectinload(Job.tag_values).selectinload(TagValue.tag),
        )
    )
    job_scalars = await async_session.scalars(job_query)
    jobs = job_scalars.all()
    assert len(jobs) == 1
    assert jobs[0].name == "insert-into_default_catalog.default_database.kafka_output"
    assert jobs[0].type == "FLINK_JOB"
    assert jobs[0].location.type == "http"
    assert jobs[0].location.name == "localhost:18081"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "http://localhost:18081"
    assert {tv.tag.name: tv.value for tv in jobs[0].tag_values} == {
        "flink.version": "1.19.0",
        "openlineage_adapter.version": "1.34.0",
    }

    job_dependency_query = select(JobDependency).order_by(JobDependency.id)
    job_dependency_scalars = await async_session.scalars(job_dependency_query)
    job_dependencies = job_dependency_scalars.all()
    assert not job_dependencies

    run_query = select(Run).order_by(Run.id).options(selectinload(Run.started_by_user))
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    run = runs[0]
    assert run.id == UUID("01965ca5-85bc-789d-93a1-9e4e48bd37b3")
    assert run.created_at == datetime(2025, 4, 22, 8, 37, 53, 724000, tzinfo=timezone.utc)
    assert run.job_id == jobs[0].id
    assert run.status == RunStatus.SUCCEEDED
    assert run.started_at == datetime(2025, 4, 22, 8, 37, 53, 938068, tzinfo=timezone.utc)
    assert run.started_by_user is None
    assert run.start_reason is None
    assert run.ended_at == datetime(2025, 4, 22, 8, 39, 53, 938068, tzinfo=timezone.utc)
    assert run.external_id == "b825f524-49d6-4dd8-bffd-3e5742c528d0"
    assert run.running_log_url == "http://localhost:18081/#/job/running/b825f524-49d6-4dd8-bffd-3e5742c528d0"
    assert run.persistent_log_url == "http://localhost:18081/#/job/completed/b825f524-49d6-4dd8-bffd-3e5742c528d0"

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    operation = operations[0]
    assert operation.id == UUID("01965ca5-85bc-789d-93a1-9e4e48bd37b3")
    assert operation.created_at == datetime(2025, 4, 22, 8, 37, 53, 724000, tzinfo=timezone.utc)
    assert operation.run_id == run.id
    assert operation.name == "insert-into_default_catalog.default_database.kafka_output"
    assert operation.type == OperationType.STREAMING
    assert operation.status == OperationStatus.SUCCEEDED
    assert operation.started_at == datetime(2025, 4, 22, 8, 37, 53, 938068, tzinfo=timezone.utc)
    assert operation.sql_query_id is None
    assert operation.ended_at == datetime(2025, 4, 22, 8, 39, 53, 938068, tzinfo=timezone.utc)
    assert operation.position is None
    assert operation.description is None

    dataset_query = (
        select(Dataset)
        .order_by(Dataset.name)
        .options(
            selectinload(Dataset.location).selectinload(Location.addresses),
        )
    )
    dataset_scalars = await async_session.scalars(dataset_query)
    datasets = dataset_scalars.all()
    assert len(datasets) == 2

    input_topic = datasets[0]
    output_topic = datasets[1]

    assert input_topic.name == "input_topic"
    assert input_topic.location.type == "kafka"
    assert input_topic.location.name == "kafka-host1:9092"
    assert len(input_topic.location.addresses) == 1
    assert input_topic.location.addresses[0].url == "kafka://kafka-host1:9092"

    assert output_topic.name == "output_topic"
    assert output_topic.location.type == "kafka"
    assert output_topic.location.name == "kafka-host2:9092"
    assert len(output_topic.location.addresses) == 1
    assert output_topic.location.addresses[0].url == "kafka://kafka-host2:9092"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert len(dataset_symlinks) == 0

    schema_query = select(Schema).order_by(Schema.digest)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert len(schemas) == 2

    input_schema = schemas[0]
    assert input_schema.fields == [
        {
            "name": "price",
            "type": "DECIMAL(38, 18)",
        },
        {
            "name": "currency",
            "type": "STRING",
        },
        {
            "name": "log_date",
            "type": "DATE",
        },
        {
            "name": "log_time",
            "type": "TIME(0)",
        },
        {
            "name": "log_ts",
            "type": "TIMESTAMP(3)",
        },
    ]

    output_schema = schemas[1]
    assert output_schema.fields == [
        {
            "name": "ts_interval",
            "type": "STRING",
        },
        {
            "name": "max_log_date",
            "type": "STRING",
        },
        {
            "name": "max_log_time",
            "type": "STRING",
        },
        {
            "name": "max_ts",
            "type": "STRING",
        },
        {
            "name": "counter",
            "type": "BIGINT",
        },
        {
            "name": "max_price",
            "type": "DECIMAL(38, 18)",
        },
    ]

    input_query = select(Input).order_by(Input.dataset_id, Input.created_at)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    kafka_input = inputs[0]
    assert kafka_input.created_at == datetime(2025, 4, 22, 8, 37, 53, 724000, tzinfo=timezone.utc)
    assert kafka_input.operation_id == operation.id
    assert kafka_input.run_id == run.id
    assert kafka_input.job_id == run.job_id
    assert kafka_input.dataset_id == input_topic.id
    assert kafka_input.schema_id == input_schema.id
    assert kafka_input.num_bytes is None
    assert kafka_input.num_rows is None
    assert kafka_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id, Output.created_at)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    kafka_output = outputs[0]
    assert kafka_output.created_at == datetime(2025, 4, 22, 8, 37, 53, 724000, tzinfo=timezone.utc)
    assert kafka_output.operation_id == operation.id
    assert kafka_output.run_id == run.id
    assert kafka_output.job_id == run.job_id
    assert kafka_output.dataset_id == output_topic.id
    assert kafka_output.type == OutputType.APPEND
    assert kafka_output.schema_id == output_schema.id
    assert kafka_output.num_bytes is None
    assert kafka_output.num_rows is None
    assert kafka_output.num_files is None

    column_lineage_query = select(ColumnLineage).order_by(ColumnLineage.id)
    column_lineage_scalars = await async_session.scalars(column_lineage_query)
    column_lineage = column_lineage_scalars.all()
    assert not column_lineage

    dataset_column_relation_query = select(DatasetColumnRelation).order_by(
        DatasetColumnRelation.type,
        DatasetColumnRelation.fingerprint,
        DatasetColumnRelation.source_column,
    )
    dataset_column_relation_scalars = await async_session.scalars(
        dataset_column_relation_query,
    )
    dataset_column_relation = dataset_column_relation_scalars.all()
    assert not dataset_column_relation
