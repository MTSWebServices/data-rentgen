from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import StarRocksExtractor
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
    OpenLineageRunFacets,
    OpenLineageStarRocksSessionInfoRunFacet,
)


def test_extractors_extract_operation_starrocks_query_started():
    query_started_at = datetime(2026, 3, 31, 19, 30, 0, 456123, tzinfo=timezone.utc)
    query_id = UUID("019d455f-d088-7d61-bdcd-6a9085a2096a")

    session_creation_time = datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=query_started_at,
        job=OpenLineageJob(
            namespace="starrocks://some-starrocks:9030",
            name="insert_query",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="STARROCKS",
                    jobType="QUERY",
                ),
                sql=OpenLineageSqlJobFacet(
                    query="INSERT INTO users_backup SELECT * FROM users WHERE active = true",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=query_id,
            facets=OpenLineageRunFacets(
                starrocks_session=OpenLineageStarRocksSessionInfoRunFacet(
                    user="myuser",
                    clientIp="11.22.33.44",
                    sessionId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
                ),
            ),
        ),
    )

    assert StarRocksExtractor().extract_operation(operation) == OperationDTO(
        id=query_id,
        run=RunDTO(
            id=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
            job=JobDTO(
                name="myuser@11.22.33.44",
                location=LocationDTO(
                    type="starrocks", name="some-starrocks:9030", addresses={"starrocks://some-starrocks:9030"}
                ),
                type=JobTypeDTO(type="STARROCKS_SESSION"),
            ),
            status=RunStatusDTO.STARTED,
            started_at=session_creation_time,
            start_reason=None,
            user=UserDTO(name="myuser"),
            ended_at=None,
            external_id=None,
            attempt=None,
            persistent_log_url=None,
            running_log_url=None,
        ),
        name="insert_query",
        description=None,
        type=OperationTypeDTO.BATCH,
        position=None,
        status=OperationStatusDTO.STARTED,
        started_at=query_started_at,
        ended_at=None,
        sql_query=SQLQueryDTO(query="INSERT INTO users_backup SELECT * FROM users WHERE active = true"),
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.COMPLETE, OperationStatusDTO.SUCCEEDED),
        (OpenLineageRunEventType.FAIL, OperationStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, OperationStatusDTO.KILLED),
    ],
)
def test_extractors_extract_operation_starrocks_query_finished(
    event_type: OpenLineageRunEventType, expected_status: OperationStatusDTO
):
    query_ended_at = datetime(2026, 3, 31, 19, 40, 0, 456123, tzinfo=timezone.utc)
    query_id = UUID("019d455f-d088-7d61-bdcd-6a9085a2096a")

    session_creation_time = datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)

    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=query_ended_at,
        job=OpenLineageJob(
            namespace="starrocks://some-starrocks:9030",
            name="insert_query",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="STARROCKS",
                    jobType="QUERY",
                ),
                sql=OpenLineageSqlJobFacet(
                    query="INSERT INTO users_backup SELECT * FROM users WHERE active = true",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=query_id,
            facets=OpenLineageRunFacets(
                starrocks_session=OpenLineageStarRocksSessionInfoRunFacet(
                    user="myuser",
                    clientIp="11.22.33.44",
                    sessionId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
                ),
            ),
        ),
    )

    assert StarRocksExtractor().extract_operation(operation) == OperationDTO(
        id=query_id,
        run=RunDTO(
            id=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
            job=JobDTO(
                name="myuser@11.22.33.44",
                location=LocationDTO(
                    type="starrocks", name="some-starrocks:9030", addresses={"starrocks://some-starrocks:9030"}
                ),
                type=JobTypeDTO(type="STARROCKS_SESSION"),
            ),
            status=RunStatusDTO.STARTED,
            started_at=session_creation_time,
            start_reason=None,
            user=UserDTO(name="myuser"),
            ended_at=None,
            external_id=None,
            attempt=None,
            persistent_log_url=None,
            running_log_url=None,
        ),
        name="insert_query",
        description=None,
        type=OperationTypeDTO.BATCH,
        position=None,
        status=expected_status,
        started_at=None,
        ended_at=query_ended_at,
        sql_query=SQLQueryDTO(query="INSERT INTO users_backup SELECT * FROM users WHERE active = true"),
    )
