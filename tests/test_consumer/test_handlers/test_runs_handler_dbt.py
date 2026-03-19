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
    SQLQuery,
    TagValue,
)

RESOURCES_PATH = Path(__file__).parent.parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_dbt() -> list[dict]:
    lines = (RESOURCES_PATH / "events_dbt.jsonl").read_text().splitlines()
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
async def test_runs_handler_dbt(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_dbt: list[dict],
    input_transformation,
):
    for event in input_transformation(events_dbt):
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
    assert jobs[0].name == "dbt-run-demo_project"
    assert jobs[0].type == "DBT_JOB"
    assert jobs[0].location.type == "local"
    assert jobs[0].location.name == "somehost"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "local://somehost"
    assert {tv.tag.name: tv.value for tv in jobs[0].tag_values} == {
        "dbt.version": "1.9.4",
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
    assert run.id == UUID("0196eccd-8aa4-7274-9116-824575596aaf")
    assert run.created_at == datetime(2025, 5, 20, 8, 26, 55, 524000, tzinfo=timezone.utc)
    assert run.job_id == jobs[0].id
    assert run.status == RunStatus.SUCCEEDED
    assert run.started_at == datetime(2025, 5, 20, 8, 26, 55, 524789, tzinfo=timezone.utc)
    assert run.started_by_user is None
    assert run.start_reason is None
    assert run.ended_at == datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    assert run.external_id == "93c69fcd-10d0-4639-a4f8-95be0da4476b"
    assert run.running_log_url is None
    assert run.persistent_log_url is None

    sql_query_query = select(SQLQuery).order_by(SQLQuery.fingerprint)
    sql_euery_scalars = await async_session.scalars(sql_query_query)
    sql_queries = sql_euery_scalars.all()
    assert len(sql_queries) == 1

    sql_query = sql_queries[0]
    assert (
        sql_query.query
        == "select\nid,\ncomplex_id,\n2023 as year,\n10 as month,\nid as day\nfrom demo_schema.source_table"
    )

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    operation = operations[0]
    assert operation.id == UUID("0196eccd-ebdc-70e3-b532-6e66f541ba29")
    assert operation.created_at == datetime(2025, 5, 20, 8, 27, 20, 412000, tzinfo=timezone.utc)
    assert operation.run_id == run.id
    assert operation.name == "demo_schema.demo_project.target_table"
    assert operation.group == "MODEL"
    assert operation.type == OperationType.BATCH
    assert operation.status == OperationStatus.SUCCEEDED
    assert operation.started_at == datetime(2025, 5, 20, 8, 27, 16, 601799, tzinfo=timezone.utc)
    assert operation.sql_query_id == sql_query.id
    assert operation.ended_at == datetime(2025, 5, 20, 8, 27, 18, 581235, tzinfo=timezone.utc)
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

    input_table = datasets[0]
    output_table = datasets[1]

    assert input_table.name == "demo_schema.source_table"
    assert input_table.location.type == "spark"
    assert input_table.location.name == "localhost:10000"
    assert len(input_table.location.addresses) == 1
    assert input_table.location.addresses[0].url == "spark://localhost:10000"

    assert output_table.name == "demo_schema.target_table"
    assert output_table.location.type == "spark"
    assert output_table.location.name == "localhost:10000"
    assert len(output_table.location.addresses) == 1
    assert output_table.location.addresses[0].url == "spark://localhost:10000"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert len(dataset_symlinks) == 0

    schema_query = select(Schema).order_by(Schema.digest)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert len(schemas) == 1

    schema = schemas[0]
    assert schema.fields == [
        {
            "name": "id",
            "description": "The primary key for this table",
        },
    ]

    input_query = select(Input).order_by(Input.dataset_id)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    kafka_input = inputs[0]
    assert kafka_input.created_at == datetime(2025, 5, 20, 8, 27, 20, 412000, tzinfo=timezone.utc)
    assert kafka_input.operation_id == operation.id
    assert kafka_input.run_id == run.id
    assert kafka_input.job_id == run.job_id
    assert kafka_input.dataset_id == input_table.id
    assert kafka_input.schema_id == schema.id
    assert kafka_input.num_bytes is None
    assert kafka_input.num_rows is None
    assert kafka_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    kafka_output = outputs[0]
    assert kafka_output.created_at == datetime(2025, 5, 20, 8, 27, 20, 412000, tzinfo=timezone.utc)
    assert kafka_output.operation_id == operation.id
    assert kafka_output.run_id == run.id
    assert kafka_output.job_id == run.job_id
    assert kafka_output.dataset_id == output_table.id
    assert kafka_output.type == OutputType.APPEND
    assert kafka_output.schema_id is None
    assert kafka_output.num_bytes is None
    assert kafka_output.num_rows == 2
    assert kafka_output.num_files is None

    column_lineage_query = select(ColumnLineage).order_by(ColumnLineage.id)
    column_lineage_scalars = await async_session.scalars(column_lineage_query)
    column_lineage = column_lineage_scalars.all()
    assert len(column_lineage) == 1

    final_column_lineage = column_lineage[0]
    assert final_column_lineage.created_at == datetime(2025, 5, 20, 8, 27, 20, 412000, tzinfo=timezone.utc)
    assert final_column_lineage.operation_id == operation.id
    assert final_column_lineage.run_id == run.id
    assert final_column_lineage.job_id == run.job_id
    assert final_column_lineage.source_dataset_id == input_table.id
    assert final_column_lineage.target_dataset_id == output_table.id

    dataset_column_relation_query = select(DatasetColumnRelation).order_by(
        DatasetColumnRelation.type,
        DatasetColumnRelation.fingerprint,
        DatasetColumnRelation.source_column,
    )
    dataset_column_relation_scalars = await async_session.scalars(
        dataset_column_relation_query,
    )
    dataset_column_relation = dataset_column_relation_scalars.all()
    assert len(dataset_column_relation) == 3

    # First event(only direct relations)
    complex_id_relation = dataset_column_relation[0]
    assert complex_id_relation.source_column == "complex_id"
    assert complex_id_relation.target_column == "complex_id"
    assert complex_id_relation.type == DatasetColumnRelationType.UNKNOWN.value
    assert complex_id_relation.fingerprint is not None

    day_relation = dataset_column_relation[1]
    assert day_relation.source_column == "id"
    assert day_relation.target_column == "day"
    assert day_relation.type == DatasetColumnRelationType.UNKNOWN.value
    assert day_relation.fingerprint is not None
    assert day_relation.fingerprint == complex_id_relation.fingerprint

    id_relation = dataset_column_relation[2]
    assert id_relation.source_column == "id"
    assert id_relation.target_column == "id"
    assert id_relation.type == DatasetColumnRelationType.UNKNOWN.value
    assert id_relation.fingerprint is not None
    assert id_relation.fingerprint == complex_id_relation.fingerprint
