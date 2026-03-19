from __future__ import annotations

from datetime import datetime, timezone

from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import HiveExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    RunStatusDTO,
    SQLQueryDTO,
    UserDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
    OpenLineageSqlJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageHiveQueryInfoRunFacet,
    OpenLineageHiveSessionInfoRunFacet,
    OpenLineageRunFacets,
)


def test_extractors_extract_operation_hive_job():
    now = datetime(2025, 6, 18, 13, 32, 10, 30000, tzinfo=timezone.utc)
    operation_id = UUID("0197833d-6cec-7609-a80f-8f4e0f8a5b1f")

    session_creation_time = datetime(2025, 6, 18, 13, 32, 3, 229000, tzinfo=timezone.utc)

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="hive://test-hadoop:10000",
            name="createtable_as_select.mydatabase.target_table",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="HIVE",
                    jobType="QUERY",
                ),
                sql=OpenLineageSqlJobFacet(
                    query="create table mydatabase.target_table as select * from mydatabase.source_table",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                hive_query=OpenLineageHiveQueryInfoRunFacet(
                    queryId="hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45",
                    operationName="CREATETABLE_AS_SELECT",
                ),
                hive_session=OpenLineageHiveSessionInfoRunFacet(
                    username="myuser",
                    clientIp="11.22.33.44",
                    sessionId="0ba6765b-3019-4172-b748-63c257158d20",
                    creationTime=session_creation_time,
                ),
            ),
        ),
    )

    assert HiveExtractor().extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=UUID("0197833d-511d-7034-be27-078fb128ca59"),  # generated
            job=JobDTO(
                name="myuser@11.22.33.44",
                location=LocationDTO(type="hive", name="test-hadoop:10000", addresses={"hive://test-hadoop:10000"}),
                type=JobTypeDTO(type="HIVE_SESSION"),
            ),
            status=RunStatusDTO.STARTED,
            started_at=session_creation_time,
            start_reason=None,
            user=UserDTO(name="myuser"),
            ended_at=None,
            external_id="0ba6765b-3019-4172-b748-63c257158d20",
            attempt=None,
            persistent_log_url=None,
            running_log_url=None,
        ),
        name="hive_20250618133205_44f7bc13-4538-42c7-a5be-8edb36c39a45",
        description="CREATETABLE_AS_SELECT",
        type=OperationTypeDTO.BATCH,
        position=None,
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
        sql_query=SQLQueryDTO(query="create table mydatabase.target_table as select * from mydatabase.source_table"),
    )
