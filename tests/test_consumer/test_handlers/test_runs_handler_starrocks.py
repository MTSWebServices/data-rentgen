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
    SQLQuery,
    TagValue,
)

RESOURCES_PATH = Path(__file__).parent.parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_starrocks() -> list[dict]:
    lines = (RESOURCES_PATH / "events_starrocks.jsonl").read_text().splitlines()
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
async def test_runs_handler_starrocks(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_starrocks: list[dict],
    input_transformation,
):
    for event in input_transformation(events_starrocks):
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
    assert jobs[0].name == "myuser@11.22.33.44"
    assert jobs[0].type == "STARROCKS_SESSION"
    assert jobs[0].location.type == "starrocks"
    assert jobs[0].location.name == "some-starrocks:9030"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "starrocks://some-starrocks:9030"
    assert not jobs[0].tag_values

    job_dependency_query = select(JobDependency).order_by(JobDependency.id)
    job_dependency_scalars = await async_session.scalars(job_dependency_query)
    job_dependencies = job_dependency_scalars.all()
    assert not job_dependencies

    run_query = select(Run).order_by(Run.id).options(selectinload(Run.started_by_user))
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    session_run = runs[0]
    assert session_run.id == UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d")
    assert session_run.created_at == datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)
    assert session_run.job_id == jobs[0].id
    assert session_run.status == RunStatus.SUCCEEDED
    assert session_run.started_at == datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)
    assert session_run.started_by_user is not None
    assert session_run.started_by_user.name == "myuser"
    assert session_run.start_reason is None
    assert session_run.ended_at == datetime(2026, 3, 31, 19, 40, 0, 456000, tzinfo=timezone.utc)
    assert session_run.external_id is None
    assert session_run.running_log_url is None
    assert session_run.persistent_log_url is None

    sql_query = select(SQLQuery).order_by(SQLQuery.fingerprint)
    sql_query_scalars = await async_session.scalars(sql_query)
    sql_queries = sql_query_scalars.all()
    assert len(sql_queries) == 1

    operation_sql_query = sql_queries[0]
    assert operation_sql_query.query == "INSERT INTO users_backup SELECT * FROM users WHERE active = true"
    assert operation_sql_query.fingerprint is not None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    query_operation = operations[0]
    assert query_operation.id == UUID("019d455f-d088-7d61-bdcd-6a9085a2096a")
    assert query_operation.created_at == datetime(2026, 3, 31, 19, 30, 0, 456000, tzinfo=timezone.utc)
    assert query_operation.run_id == session_run.id
    assert query_operation.name == "insert_query"
    assert query_operation.description is None
    assert query_operation.type == OperationType.BATCH
    assert query_operation.status == OperationStatus.SUCCEEDED
    assert query_operation.started_at == datetime(2026, 3, 31, 19, 30, 0, 456000, tzinfo=timezone.utc)
    assert query_operation.sql_query_id == operation_sql_query.id
    assert query_operation.ended_at == datetime(2026, 3, 31, 19, 40, 0, 456000, tzinfo=timezone.utc)
    assert query_operation.position is None

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

    starrocks_source_table = datasets[0]
    starrocks_target_table = datasets[1]

    assert starrocks_source_table.name == "test_db.test_table"
    assert starrocks_source_table.location.type == "http"
    assert starrocks_source_table.location.name == "test-iceberg:8181"
    assert len(starrocks_source_table.location.addresses) == 1
    assert starrocks_source_table.location.addresses[0].url == "http://test-iceberg:8181"

    assert starrocks_target_table.name == "test_db.users_backup"
    assert starrocks_target_table.location.type == "http"
    assert starrocks_target_table.location.name == "test-iceberg:8181"
    assert len(starrocks_target_table.location.addresses) == 1
    assert starrocks_target_table.location.addresses[0].url == "http://test-iceberg:8181"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert not dataset_symlinks

    schema_query = select(Schema).order_by(Schema.digest)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert not schemas

    input_query = select(Input).order_by(Input.dataset_id)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    starrocks_input = inputs[0]
    assert starrocks_input.created_at == datetime(2026, 3, 31, 19, 30, 0, 456000, tzinfo=timezone.utc)
    assert starrocks_input.operation_id == query_operation.id
    assert starrocks_input.run_id == session_run.id
    assert starrocks_input.job_id == session_run.job_id
    assert starrocks_input.dataset_id == starrocks_source_table.id
    assert starrocks_input.schema_id is None
    assert starrocks_input.num_bytes == 524288
    assert starrocks_input.num_rows == 10000
    assert starrocks_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    starrocks_output = outputs[0]
    assert starrocks_output.created_at == datetime(2026, 3, 31, 19, 30, 0, 456000, tzinfo=timezone.utc)
    assert starrocks_output.operation_id == query_operation.id
    assert starrocks_output.run_id == session_run.id
    assert starrocks_output.job_id == session_run.job_id
    assert starrocks_output.dataset_id == starrocks_target_table.id
    assert starrocks_output.type == OutputType.APPEND
    assert starrocks_output.schema_id is None
    assert starrocks_output.num_bytes is None
    assert starrocks_output.num_rows == 8500
    assert starrocks_output.num_files is None

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
