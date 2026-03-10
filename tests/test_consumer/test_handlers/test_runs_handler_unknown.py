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
    DatasetColumnRelationType,
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
def events_unknown() -> list[dict]:
    lines = (RESOURCES_PATH / "events_unknown.jsonl").read_text().splitlines()
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
async def test_runs_handler_unknown(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_unknown: list[dict],
    input_transformation,
):
    for event in input_transformation(events_unknown):
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

    job = jobs[0]
    assert job.name == "somejob"
    assert job.type == "UNKNOWN_SOMETHING"
    assert job.location.type == "unknown"
    assert job.location.name == "unknown"
    assert len(job.location.addresses) == 1
    assert job.location.addresses[0].url == "unknown://unknown"
    assert {tv.tag.name: tv.value for tv in jobs[0].tag_values} == {}

    job_dependency_query = select(JobDependency).order_by(JobDependency.id)
    job_dependency_scalars = await async_session.scalars(job_dependency_query)
    job_dependencies = job_dependency_scalars.all()
    assert not job_dependencies

    run_query = select(Run).order_by(Run.id).options(selectinload(Run.started_by_user))
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    run = runs[0]
    assert run.id == UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    assert run.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert run.job_id == job.id
    assert run.status == RunStatus.SUCCEEDED
    assert run.started_at == datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    assert run.ended_at == datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    assert run.started_by_user is None
    assert run.start_reason is None
    assert run.external_id is None
    assert run.running_log_url is None
    assert run.persistent_log_url is None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    operation = operations[0]
    assert operation.id == UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    assert operation.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert operation.run_id == run.id
    assert operation.name == "somejob"
    assert operation.type == OperationType.BATCH
    assert operation.status == OperationStatus.SUCCEEDED
    assert operation.started_at is None  # first event didn't have any inputs or outputs
    assert operation.ended_at == datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
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

    hive_table = datasets[0]
    clickhouse_table = datasets[1]

    assert clickhouse_table.name == "mydb.myschema.mytable"
    assert clickhouse_table.location.type == "clickhouse"
    assert clickhouse_table.location.name == "localhost:8123"
    assert len(clickhouse_table.location.addresses) == 1
    assert clickhouse_table.location.addresses[0].url == "clickhouse://localhost:8123"

    assert hive_table.name == "mydatabase.source_table"
    assert hive_table.location.type == "hive"
    assert hive_table.location.name == "test-hadoop:9083"
    assert len(hive_table.location.addresses) == 1
    assert hive_table.location.addresses[0].url == "hive://test-hadoop:9083"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert len(dataset_symlinks) == 0

    schema_query = select(Schema).order_by(Schema.digest)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert len(schemas) == 2

    clickhouse_schema = schemas[0]
    assert clickhouse_schema.fields == [
        {"name": "dt", "type": "timestamp"},
        {"name": "customer_id", "type": "decimal(20,0)"},
        {"name": "total_spent", "type": "float"},
    ]

    hive_schema = schemas[1]
    assert hive_schema.fields == [
        {"name": "dt", "type": "timestamp", "description": "Business date"},
        {"name": "customer_id", "type": "decimal(20,0)"},
        {"name": "total_spent", "type": "float"},
    ]

    input_query = select(Input).order_by(Input.dataset_id)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    hive_input = inputs[0]
    assert hive_input.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert hive_input.operation_id == operation.id
    assert hive_input.run_id == run.id
    assert hive_input.job_id == run.job_id
    assert hive_input.dataset_id == hive_table.id
    assert hive_input.schema_id == hive_schema.id
    assert hive_input.num_bytes is None
    assert hive_input.num_rows is None
    assert hive_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    clickhouse_output = outputs[0]
    assert clickhouse_output.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert clickhouse_output.operation_id == operation.id
    assert clickhouse_output.run_id == run.id
    assert clickhouse_output.job_id == run.job_id
    assert clickhouse_output.dataset_id == clickhouse_table.id
    assert clickhouse_output.type == OutputType.OVERWRITE
    assert clickhouse_output.schema_id == clickhouse_schema.id
    assert clickhouse_output.num_bytes == 5_000_000
    assert clickhouse_output.num_rows == 10_000
    assert clickhouse_output.num_files is None

    column_lineage_query = select(ColumnLineage).order_by(ColumnLineage.id)
    column_lineage_scalars = await async_session.scalars(column_lineage_query)
    column_lineage = column_lineage_scalars.all()
    # There are two rows in column_lineage table, for two events.
    # One with direct column lineage and second with direct + indirect.
    # Difference between them should be only in fingerprint
    assert len(column_lineage) == 2

    first_event_column_lineage = column_lineage[0]
    assert first_event_column_lineage.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert first_event_column_lineage.operation_id == operation.id
    assert first_event_column_lineage.run_id == run.id
    assert first_event_column_lineage.job_id == run.job_id
    assert first_event_column_lineage.source_dataset_id == hive_table.id
    assert first_event_column_lineage.target_dataset_id == clickhouse_table.id

    second_event_column_lineage = column_lineage[1]
    assert second_event_column_lineage.created_at == datetime(2024, 7, 5, 9, 5, 49, 584000, tzinfo=timezone.utc)
    assert second_event_column_lineage.operation_id == operation.id
    assert second_event_column_lineage.run_id == run.id
    assert second_event_column_lineage.job_id == run.job_id
    assert second_event_column_lineage.source_dataset_id == hive_table.id
    assert second_event_column_lineage.target_dataset_id == clickhouse_table.id

    dataset_column_relation_query = select(DatasetColumnRelation).order_by(
        DatasetColumnRelation.type,
        DatasetColumnRelation.fingerprint,
        DatasetColumnRelation.source_column,
    )
    dataset_column_relation_scalars = await async_session.scalars(
        dataset_column_relation_query,
    )
    dataset_column_relation = dataset_column_relation_scalars.all()
    # In case rows order by type: first 5 rows correspond to direct lineage relations and last to indirect
    # Ordering by fingerprint separate one event from another
    assert len(dataset_column_relation) == 7

    # First event(only direct relations)
    customer_id_relation = dataset_column_relation[0]
    assert customer_id_relation.source_column == "customer_id"
    assert customer_id_relation.target_column == "customer_id"
    assert customer_id_relation.type == DatasetColumnRelationType.IDENTITY.value

    dt_relation = dataset_column_relation[1]
    assert dt_relation.source_column == "dt"
    assert dt_relation.target_column == "dt"
    assert dt_relation.type == DatasetColumnRelationType.IDENTITY.value

    total_spent_relation = dataset_column_relation[2]
    assert total_spent_relation.source_column == "total_spent"
    assert total_spent_relation.target_column == "total_spent"
    assert total_spent_relation.type == DatasetColumnRelationType.IDENTITY.value
    fingerpints = [
        customer_id_relation.fingerprint,
        dt_relation.fingerprint,
        total_spent_relation.fingerprint,
    ]
    assert fingerpints[0] is not None
    assert all(fingerprint == fingerpints[0] for fingerprint in fingerpints)

    # Second event(direct and indirect relations)
    customer_id_relation = dataset_column_relation[3]
    assert customer_id_relation.source_column == "customer_id"
    assert customer_id_relation.target_column == "customer_id"
    assert customer_id_relation.type == DatasetColumnRelationType.IDENTITY.value

    dt_relation = dataset_column_relation[4]
    assert dt_relation.source_column == "dt"
    assert dt_relation.target_column == "dt"
    assert dt_relation.type == DatasetColumnRelationType.IDENTITY.value

    total_spent_relation = dataset_column_relation[5]
    assert total_spent_relation.source_column == "total_spent"
    assert total_spent_relation.target_column == "total_spent"
    assert total_spent_relation.type == DatasetColumnRelationType.IDENTITY.value

    # Indirect relation
    customer_id_indirect_relation = dataset_column_relation[6]
    assert customer_id_indirect_relation.target_column is None
    assert customer_id_indirect_relation.source_column == "customer_id"
    assert customer_id_indirect_relation.type == DatasetColumnRelationType.JOIN.value

    fingerpints = [
        customer_id_indirect_relation.fingerprint,
        customer_id_relation.fingerprint,
        dt_relation.fingerprint,
        total_spent_relation.fingerprint,
    ]
    assert fingerpints[0] is not None
    assert all(fingerprint == fingerpints[0] for fingerprint in fingerpints)
