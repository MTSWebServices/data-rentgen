from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import FlinkExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    OperationDTO,
    OperationStatusDTO,
    OperationTypeDTO,
    RunDTO,
    RunStatusDTO,
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
    OpenLineageFlinkJobDetailsRunFacet,
    OpenLineageRunFacets,
)


def test_extractors_extract_operation_flink_job():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://localhost:18081",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.STREAMING,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                flink_job=OpenLineageFlinkJobDetailsRunFacet(
                    jobId="b825f524-49d6-4dd8-bffd-3e5742c528d0",
                ),
            ),
        ),
    )

    assert FlinkExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                type=JobTypeDTO(type="FLINK_JOB"),
                location=LocationDTO(
                    type="http",
                    name="localhost:18081",
                    addresses={"http://localhost:18081"},
                ),
            ),
            status=RunStatusDTO.STARTED,
            started_at=now,
            ended_at=None,
            external_id="b825f524-49d6-4dd8-bffd-3e5742c528d0",
            running_log_url="http://localhost:18081/#/job/running/b825f524-49d6-4dd8-bffd-3e5742c528d0",
            persistent_log_url="http://localhost:18081/#/job/completed/b825f524-49d6-4dd8-bffd-3e5742c528d0",
        ),
        name="myjob",
        type=OperationTypeDTO.STREAMING,
        position=None,
        description=None,
        status=OperationStatusDTO.STARTED,
        started_at=now,
        ended_at=None,
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.COMPLETE, OperationStatusDTO.SUCCEEDED),
        (OpenLineageRunEventType.FAIL, OperationStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, OperationStatusDTO.KILLED),
        (OpenLineageRunEventType.OTHER, OperationStatusDTO.UNKNOWN),
    ],
)
def test_extractors_extract_operation_flink_job_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://localhost:18081",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="FLINK",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                flink_job=OpenLineageFlinkJobDetailsRunFacet(
                    jobId="b825f524-49d6-4dd8-bffd-3e5742c528d0",
                ),
            ),
        ),
    )

    ended_at = now if expected_status != OperationStatusDTO.UNKNOWN else None

    assert FlinkExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                type=JobTypeDTO(type="FLINK_JOB"),
                location=LocationDTO(
                    type="http",
                    name="localhost:18081",
                    addresses={"http://localhost:18081"},
                ),
            ),
            status=RunStatusDTO(expected_status),
            started_at=None,
            ended_at=ended_at,
            external_id="b825f524-49d6-4dd8-bffd-3e5742c528d0",
            persistent_log_url="http://localhost:18081/#/job/completed/b825f524-49d6-4dd8-bffd-3e5742c528d0",
            running_log_url="http://localhost:18081/#/job/running/b825f524-49d6-4dd8-bffd-3e5742c528d0",
        ),
        name="myjob",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )
