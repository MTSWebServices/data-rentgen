from datetime import datetime, timezone

import pytest
from uuid6 import UUID

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


@pytest.fixture
def starrocks_session_job() -> OpenLineageJob:
    return OpenLineageJob(
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
    )


@pytest.fixture
def starrocks_session_run() -> OpenLineageRun:
    return OpenLineageRun(
        runId=UUID("019d455f-d088-7d61-bdcd-6a9085a2096a"),
        facets=OpenLineageRunFacets(
            starrocks_session=OpenLineageStarRocksSessionInfoRunFacet(
                user="myuser",
                clientIp="11.22.33.44",
                sessionId=UUID("019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"),
            ),
        ),
    )


@pytest.fixture
def starrocks_query_run_event_start(
    starrocks_session_job: OpenLineageJob, starrocks_session_run: OpenLineageRun
) -> OpenLineageRunEvent:
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=datetime(2026, 3, 31, 19, 30, 0, 456123, tzinfo=timezone.utc),
        job=starrocks_session_job,
        run=starrocks_session_run,
    )


@pytest.fixture
def starrocks_query_run_event_stop(
    starrocks_session_job: OpenLineageJob, starrocks_session_run: OpenLineageRun
) -> OpenLineageRunEvent:
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=datetime(2026, 3, 31, 19, 40, 0, 456123, tzinfo=timezone.utc),
        job=starrocks_session_job,
        run=starrocks_session_run,
    )
