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
    DatasetSymlinkType,
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
def events_hive() -> list[dict]:
    lines = (RESOURCES_PATH / "events_hive.jsonl").read_text().splitlines()
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
async def test_runs_handler_hive(
    test_broker: KafkaBroker,
    async_session: AsyncSession,
    events_hive: list[dict],
    input_transformation,
):
    for event in input_transformation(events_hive):
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
    assert jobs[0].type == "HIVE_SESSION"
    assert jobs[0].location.type == "hive"
    assert jobs[0].location.name == "test-hadoop:10000"
    assert len(jobs[0].location.addresses) == 1
    assert jobs[0].location.addresses[0].url == "hive://test-hadoop:10000"
    assert {tv.tag.name: tv.value for tv in jobs[0].tag_values} == {
        "hive.version": "3.1.3",
        "openlineage_adapter.version": "1.35.0",
    }

    job_dependency_query = select(JobDependency).order_by(JobDependency.id)
    job_dependency_scalars = await async_session.scalars(job_dependency_query)
    job_dependencies = job_dependency_scalars.all()
    assert not job_dependencies

    run_query = select(Run).order_by(Run.id).options(selectinload(Run.started_by_user))
    run_scalars = await async_session.scalars(run_query)
    runs = run_scalars.all()
    assert len(runs) == 1

    session_run = runs[0]
    assert session_run.id == UUID("0197833d-511d-7034-be27-078fb128ca59")  # generated
    assert session_run.created_at == datetime(2025, 6, 18, 13, 32, 3, 229000, tzinfo=timezone.utc)
    assert session_run.job_id == jobs[0].id
    assert session_run.status == RunStatus.STARTED
    assert session_run.started_at == datetime(2025, 6, 18, 13, 32, 3, 229000, tzinfo=timezone.utc)
    assert session_run.started_by_user is not None
    assert session_run.started_by_user.name == "myuser"
    assert session_run.start_reason is None
    assert session_run.ended_at is None
    assert session_run.external_id == "0ba6765b-3019-4172-b748-63c257158d20"
    assert session_run.running_log_url is None
    assert session_run.persistent_log_url is None

    sql_query = select(SQLQuery).order_by(SQLQuery.fingerprint)
    sql_query_scalars = await async_session.scalars(sql_query)
    sql_queries = sql_query_scalars.all()
    assert len(sql_queries) == 1

    operation_sql_query = sql_queries[0]
    assert operation_sql_query.query == "create table mydatabase.target_table as select * from mydatabase.source_table"
    assert operation_sql_query.fingerprint is not None

    operation_query = select(Operation).order_by(Operation.id)
    operation_scalars = await async_session.scalars(operation_query)
    operations = operation_scalars.all()
    assert len(operations) == 1

    query_operation = operations[0]
    assert query_operation.id == UUID("0197833d-6cec-7609-a80f-8f4e0f8a5b1f")
    assert query_operation.created_at == datetime(2025, 6, 18, 13, 32, 10, 348000, tzinfo=timezone.utc)
    assert query_operation.run_id == session_run.id
    assert query_operation.name == "hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45"
    assert query_operation.description == "CREATETABLE_AS_SELECT"
    assert query_operation.type == OperationType.BATCH
    assert query_operation.status == OperationStatus.SUCCEEDED
    assert query_operation.started_at is None
    assert query_operation.sql_query_id == operation_sql_query.id
    assert query_operation.ended_at == datetime(2025, 6, 18, 13, 32, 10, 30000, tzinfo=timezone.utc)
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
    assert len(datasets) == 4

    hdfs_source_path = datasets[0]
    hdfs_target_path = datasets[1]
    hive_source_table = datasets[2]
    hive_target_table = datasets[3]

    assert hive_source_table.name == "mydatabase.source_table"
    assert hive_source_table.location.type == "hive"
    assert hive_source_table.location.name == "test-hadoop:9083"
    assert len(hive_source_table.location.addresses) == 1
    assert hive_source_table.location.addresses[0].url == "hive://test-hadoop:9083"

    assert hive_target_table.name == "mydatabase.target_table"
    assert hive_target_table.location.type == "hive"
    assert hive_target_table.location.name == "test-hadoop:9083"
    assert len(hive_target_table.location.addresses) == 1
    assert hive_target_table.location.addresses[0].url == "hive://test-hadoop:9083"

    assert hdfs_source_path.name == "/user/hive/warehouse/mydatabase.db/source_table"
    assert hdfs_source_path.location.type == "hdfs"
    assert hdfs_source_path.location.name == "test-hadoop:9820"
    assert len(hdfs_source_path.location.addresses) == 1
    assert hdfs_source_path.location.addresses[0].url == "hdfs://test-hadoop:9820"

    assert hdfs_target_path.name == "/user/hive/warehouse/mydatabase.db/target_table"
    assert hdfs_target_path.location.type == "hdfs"
    assert hdfs_target_path.location.name == "test-hadoop:9820"
    assert len(hdfs_target_path.location.addresses) == 1
    assert hdfs_target_path.location.addresses[0].url == "hdfs://test-hadoop:9820"

    dataset_symlink_query = select(DatasetSymlink).order_by(DatasetSymlink.type)
    dataset_symlink_scalars = await async_session.scalars(dataset_symlink_query)
    dataset_symlinks = dataset_symlink_scalars.all()
    assert len(dataset_symlinks) == 4

    assert dataset_symlinks[0].from_dataset_id == hdfs_source_path.id
    assert dataset_symlinks[0].to_dataset_id == hive_source_table.id
    assert dataset_symlinks[0].type == DatasetSymlinkType.METASTORE

    assert dataset_symlinks[1].from_dataset_id == hdfs_target_path.id
    assert dataset_symlinks[1].to_dataset_id == hive_target_table.id
    assert dataset_symlinks[1].type == DatasetSymlinkType.METASTORE

    assert dataset_symlinks[2].from_dataset_id == hive_source_table.id
    assert dataset_symlinks[2].to_dataset_id == hdfs_source_path.id
    assert dataset_symlinks[2].type == DatasetSymlinkType.WAREHOUSE

    assert dataset_symlinks[3].from_dataset_id == hive_target_table.id
    assert dataset_symlinks[3].to_dataset_id == hdfs_target_path.id
    assert dataset_symlinks[3].type == DatasetSymlinkType.WAREHOUSE

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
    assert hive_input.created_at == datetime(2025, 6, 18, 13, 32, 10, 348000, tzinfo=timezone.utc)
    assert hive_input.operation_id == query_operation.id
    assert hive_input.run_id == session_run.id
    assert hive_input.job_id == session_run.job_id
    assert hive_input.dataset_id == hive_source_table.id
    assert hive_input.schema_id == hive_schema.id
    assert hive_input.num_bytes is None
    assert hive_input.num_rows is None
    assert hive_input.num_files is None

    output_query = select(Output).order_by(Output.dataset_id)
    output_scalars = await async_session.scalars(output_query)
    outputs = output_scalars.all()
    assert len(outputs) == 1

    hive_output = outputs[0]
    assert hive_output.created_at == datetime(2025, 6, 18, 13, 32, 10, 348000, tzinfo=timezone.utc)
    assert hive_output.operation_id == query_operation.id
    assert hive_output.run_id == session_run.id
    assert hive_output.job_id == session_run.job_id
    assert hive_output.dataset_id == hive_target_table.id
    assert hive_output.type == OutputType.CREATE
    assert hive_output.schema_id == clickhouse_schema.id
    assert hive_output.num_bytes is None
    assert hive_output.num_rows is None
    assert hive_output.num_files is None

    column_lineage_query = select(ColumnLineage).order_by(ColumnLineage.id)
    column_lineage_scalars = await async_session.scalars(column_lineage_query)
    column_lineage = column_lineage_scalars.all()
    assert len(column_lineage) == 1

    first_column_lineage = column_lineage[0]
    assert first_column_lineage.created_at == datetime(2025, 6, 18, 13, 32, 10, 348000, tzinfo=timezone.utc)
    assert first_column_lineage.operation_id == query_operation.id
    assert first_column_lineage.run_id == session_run.id
    assert first_column_lineage.job_id == session_run.job_id
    assert first_column_lineage.source_dataset_id == hive_source_table.id
    assert first_column_lineage.target_dataset_id == hive_target_table.id

    dataset_column_relation_query = select(DatasetColumnRelation).order_by(
        DatasetColumnRelation.type,
        DatasetColumnRelation.fingerprint,
        DatasetColumnRelation.source_column,
    )
    dataset_column_relation_scalars = await async_session.scalars(
        dataset_column_relation_query,
    )
    dataset_column_relation = dataset_column_relation_scalars.all()
    # In case rows order by type: first 3 rows correspond to direct lineage relations and last to indirect.
    # All with same fingerprint
    assert len(dataset_column_relation) == 4

    # First event(only direct relations)
    customer_id_relation = dataset_column_relation[0]
    assert customer_id_relation.source_column == "customer_id"
    assert customer_id_relation.target_column == "customer_id"
    assert customer_id_relation.type == DatasetColumnRelationType.IDENTITY.value
    assert customer_id_relation.fingerprint is not None

    dt_relation = dataset_column_relation[1]
    assert dt_relation.source_column == "dt"
    assert dt_relation.target_column == "dt"
    assert dt_relation.type == DatasetColumnRelationType.IDENTITY.value
    assert dt_relation.fingerprint is not None
    assert dt_relation.fingerprint == customer_id_relation.fingerprint

    total_spent_relation = dataset_column_relation[2]
    assert total_spent_relation.source_column == "total_spent"
    assert total_spent_relation.target_column == "total_spent"
    assert total_spent_relation.type == DatasetColumnRelationType.IDENTITY.value
    assert total_spent_relation.fingerprint is not None
    assert total_spent_relation.fingerprint == customer_id_relation.fingerprint

    # Indirect relation
    customer_id_indirect_relation = dataset_column_relation[3]
    assert customer_id_indirect_relation.target_column is None
    assert customer_id_indirect_relation.source_column == "customer_id"
    assert customer_id_indirect_relation.type == DatasetColumnRelationType.JOIN.value
    assert customer_id_indirect_relation.fingerprint is not None
    assert customer_id_indirect_relation.fingerprint == customer_id_relation.fingerprint
