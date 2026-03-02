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
    Location,
    Operation,
    OperationStatus,
    OperationType,
    Output,
    OutputType,
    Run,
    RunStartReason,
    RunStatus,
    Schema,
    SQLQuery,
    TagValue,
    User,
)

RESOURCES_PATH = Path(__file__).parent.parent.parent.joinpath("resources").resolve()

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


@pytest.fixture
def events_airflow() -> list[dict]:
    lines = (RESOURCES_PATH / "events_airflow.jsonl").read_text().splitlines()
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
async def test_runs_handler_airflow(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_airflow: list[dict],
    input_transformation,
):
    for event in input_transformation(events_airflow):
        await test_broker.publish(event, "input.runs")

    # both Airflow DAG in Task have the same location
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

    expected_tag_values = {
        "airflow.version": "2.10.5",
        "openlineage_adapter.version": "2.3.0",
        "environment": "production",
    }

    assert len(jobs) == 2
    assert jobs[0].name == "mydag"
    assert jobs[0].type == "AIRFLOW_DAG"
    assert jobs[0].location.type == "http"
    assert jobs[0].location.name == "airflow-host:8081"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "http://airflow-host:8081"
    assert {tv.tag.name: tv.value for tv in jobs[0].tag_values} == expected_tag_values
    assert jobs[0].parent_job_id is None

    assert jobs[1].name == "mydag.mytask"
    assert jobs[1].type == "AIRFLOW_TASK"
    assert jobs[1].location == jobs[0].location
    assert {tv.tag.name: tv.value for tv in jobs[1].tag_values} == expected_tag_values
    assert jobs[1].parent_job_id == jobs[0].id

    run_query = select(Run).order_by(Run.id)
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 2

    user_query = select(User).where(User.name == "myuser")
    user_scalars = await async_session.scalars(user_query)
    user = user_scalars.one_or_none()

    dag_run = runs[0]
    assert dag_run.id == UUID("01908223-0782-79b8-9495-b1c38aaee839")
    assert dag_run.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert dag_run.job_id == jobs[0].id
    assert dag_run.status == RunStatus.SUCCEEDED
    assert dag_run.started_at == datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    assert dag_run.ended_at == datetime(2024, 7, 5, 9, 8, 5, 691973, tzinfo=timezone.utc)
    assert dag_run.started_by_user_id is not None
    assert dag_run.started_by_user_id == user.id
    assert dag_run.start_reason == RunStartReason.MANUAL
    assert dag_run.end_reason is None
    assert dag_run.external_id == "manual__2024-07-05T09:04:12.162809+00:00"
    assert dag_run.attempt is None
    assert dag_run.persistent_log_url == (
        "http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A12.162809%2B00%3A00"
    )
    assert dag_run.expected_start_at == datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc)
    assert dag_run.expected_end_at is None

    task_run = runs[1]
    assert task_run.id == UUID("01908223-0782-7fc0-9d69-b1df9dac2c60")
    assert task_run.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert task_run.job_id == jobs[1].id
    assert task_run.parent_run_id == dag_run.id
    assert task_run.status == RunStatus.SUCCEEDED
    assert task_run.started_at == datetime(2024, 7, 5, 9, 4, 20, 783845, tzinfo=timezone.utc)
    assert task_run.ended_at == datetime(2024, 7, 5, 9, 7, 37, 858423, tzinfo=timezone.utc)
    assert task_run.started_by_user_id is not None
    assert task_run.started_by_user_id == user.id
    assert task_run.start_reason == RunStartReason.MANUAL
    assert task_run.end_reason is None
    assert task_run.external_id == "manual__2024-07-05T09:04:12.162809+00:00"
    assert task_run.attempt == "1"
    assert task_run.persistent_log_url == (
        "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A12.162809%2B00%3A00&task_id=mytask&map_index=-1"
    )
    assert task_run.running_log_url is None
    assert task_run.expected_start_at == datetime(2024, 7, 5, 9, 4, 12, 162809, tzinfo=timezone.utc)
    assert task_run.expected_end_at is None

    sql_query = select(SQLQuery).order_by(SQLQuery.id)
    sql_query_scalars = await async_session.scalars(sql_query)
    sql_queries = sql_query_scalars.all()
    assert len(sql_queries) == 1

    operation_sql_query = sql_queries[0]
    assert operation_sql_query.query == (
        "INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)\n"
        "SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,\n"
        "       order_placed_on,\n"
        "       COUNT(*) AS orders_placed\n"
        "  FROM top_delivery_times\n"
        " GROUP BY order_placed_on"
    )
    assert operation_sql_query.fingerprint is not None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    operation_query = select(Operation)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()

    task_operation = operations[0]
    assert task_operation.id == UUID("01908223-0782-7fc0-9d69-b1df9dac2c60")  # same id and created_at
    assert task_operation.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert task_operation.run_id == task_run.id
    assert task_operation.name == "mytask"
    assert task_operation.type == OperationType.BATCH
    assert task_operation.status == OperationStatus.SUCCEEDED
    assert task_operation.started_at == datetime(2024, 7, 5, 9, 4, 20, 783845, tzinfo=timezone.utc)
    assert task_operation.sql_query_id == operation_sql_query.id
    assert task_operation.ended_at == datetime(2024, 7, 5, 9, 7, 37, 858423, tzinfo=timezone.utc)
    assert task_operation.description == "SQLExecuteQueryOperator"
    assert task_operation.position is None
    assert task_operation.group is None

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

    input_table = datasets[1]
    assert input_table.name == "food_delivery.public.top_delivery_times"
    assert input_table.location.type == "postgres"
    assert input_table.location.name == "postgres:5432"
    assert len(input_table.location.addresses) == 1
    assert input_table.location.addresses[0].url == "postgres://postgres:5432"

    output_table = datasets[0]
    assert output_table.name == "food_delivery.public.popular_orders_day_of_week"
    assert output_table.location.type == "postgres"
    assert output_table.location.name == "postgres:5432"
    assert len(output_table.location.addresses) == 1
    assert output_table.location.addresses[0].url == "postgres://postgres:5432"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert not dataset_symlinks

    schema_query = select(Schema).order_by(Schema.digest)
    schema_scalars = await async_session.scalars(schema_query)
    schemas = schema_scalars.all()
    assert len(schemas) == 2

    input_schema = schemas[1]
    assert input_schema.fields == [
        {"name": "order_id", "type": "integer"},
        {"name": "order_placed_on", "type": "timestamp"},
        {"name": "order_dispatched_on", "type": "timestamp"},
        {"name": "order_delivery_time", "type": "double precision"},
    ]

    output_schema = schemas[0]
    assert output_schema.fields == [
        {"name": "order_day_of_week", "type": "varchar"},
        {"name": "order_placed_on", "type": "timestamp"},
        {"name": "orders_placed", "type": "int4"},
    ]

    input_query = select(Input).order_by(Input.dataset_id)
    input_scalars = await async_session.scalars(input_query)
    inputs = input_scalars.all()
    assert len(inputs) == 1

    postgres_input = inputs[0]
    assert postgres_input.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert postgres_input.operation_id == task_operation.id
    assert postgres_input.run_id == task_run.id
    assert postgres_input.job_id == task_run.job_id
    assert postgres_input.dataset_id == input_table.id
    assert postgres_input.schema_id == input_schema.id
    assert postgres_input.num_bytes is None
    assert postgres_input.num_rows is None
    assert postgres_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    postgres_output = outputs[0]
    assert postgres_output.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert postgres_output.operation_id == task_operation.id
    assert postgres_output.run_id == task_run.id
    assert postgres_output.job_id == task_run.job_id
    assert postgres_output.dataset_id == output_table.id
    assert postgres_output.type == OutputType.APPEND
    assert postgres_output.schema_id == output_schema.id
    assert postgres_output.num_bytes is None
    assert postgres_output.num_rows is None
    assert postgres_output.num_files is None

    column_lineage_query = select(ColumnLineage).order_by(ColumnLineage.id)
    column_lineage_scalars = await async_session.scalars(column_lineage_query)
    column_lineage = column_lineage_scalars.all()
    assert len(column_lineage) == 1

    direct_column_lineage = column_lineage[0]
    assert direct_column_lineage.created_at == datetime(2024, 7, 5, 9, 4, 12, 162000, tzinfo=timezone.utc)
    assert direct_column_lineage.operation_id == task_operation.id
    assert direct_column_lineage.run_id == task_run.id
    assert direct_column_lineage.job_id == task_run.job_id
    assert direct_column_lineage.source_dataset_id == input_table.id
    assert direct_column_lineage.target_dataset_id == output_table.id

    dataset_column_relation_query = select(DatasetColumnRelation).order_by(
        DatasetColumnRelation.type,
        DatasetColumnRelation.fingerprint,
        DatasetColumnRelation.source_column,
    )
    dataset_column_relation_scalars = await async_session.scalars(
        dataset_column_relation_query,
    )
    dataset_column_relation = dataset_column_relation_scalars.all()
    assert len(dataset_column_relation) == 2

    # First event(only direct relations)
    order_day_of_week_relation = dataset_column_relation[0]
    assert order_day_of_week_relation.source_column == "order_placed_on"
    assert order_day_of_week_relation.target_column == "order_day_of_week"
    assert order_day_of_week_relation.type == DatasetColumnRelationType.UNKNOWN.value
    assert order_day_of_week_relation.fingerprint is not None

    order_placed_on_relation = dataset_column_relation[1]
    assert order_placed_on_relation.source_column == "order_placed_on"
    assert order_placed_on_relation.target_column == "order_placed_on"
    assert order_placed_on_relation.type == DatasetColumnRelationType.UNKNOWN.value
    assert order_placed_on_relation.fingerprint is not None
    assert order_placed_on_relation.fingerprint == order_day_of_week_relation.fingerprint
