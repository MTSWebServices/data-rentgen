from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import StarRocksExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStatusDTO,
    UserDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
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


def test_extractors_extract_run_starrocks_query():
    session_creation_time = datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=datetime(2026, 3, 31, 19, 40, 0, 456123, tzinfo=timezone.utc),
        job=OpenLineageJob(
            namespace="starrocks://some-starrocks:9030",
            name="insert_query",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="STARROCKS",
                    jobType="QUERY",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("019d455f-d088-7d61-bdcd-6a9085a2096a"),
            facets=OpenLineageRunFacets(
                starrocks_session=OpenLineageStarRocksSessionInfoRunFacet(
                    user="myuser",
                    clientIp="11.22.33.44",
                    sessionId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
                ),
            ),
        ),
    )
    assert StarRocksExtractor().extract_run(run) == RunDTO(
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
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.COMPLETE, RunStatusDTO.SUCCEEDED),
        (OpenLineageRunEventType.FAIL, RunStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, RunStatusDTO.KILLED),
    ],
)
def test_extractors_extract_run_starrocks_session(event_type: OpenLineageRunEventType, expected_status: RunStatusDTO):
    session_creation_time = datetime(2026, 3, 31, 19, 30, 0, 106000, tzinfo=timezone.utc)
    session_end_time = datetime(2026, 3, 31, 19, 40, 0, 456000, tzinfo=timezone.utc)
    run = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=session_end_time,
        job=OpenLineageJob(
            namespace="starrocks://some-starrocks:9030",
            name="session",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="STARROCKS",
                    jobType="SESSION",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
            facets=OpenLineageRunFacets(
                starrocks_session=OpenLineageStarRocksSessionInfoRunFacet(
                    user="myuser",
                    clientIp="11.22.33.44",
                    sessionId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
                ),
            ),
        ),
    )
    assert StarRocksExtractor().extract_run(run) == RunDTO(
        id=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
        job=JobDTO(
            name="myuser@11.22.33.44",
            location=LocationDTO(
                type="starrocks", name="some-starrocks:9030", addresses={"starrocks://some-starrocks:9030"}
            ),
            type=JobTypeDTO(type="STARROCKS_SESSION"),
        ),
        status=expected_status,
        started_at=session_creation_time,
        start_reason=None,
        user=UserDTO(name="myuser"),
        ended_at=session_end_time,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
