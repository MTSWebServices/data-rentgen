from __future__ import annotations

from datetime import datetime, timezone

from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import FlinkExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
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
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)


def test_extractors_extract_run_flink():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
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
                # job tags are missing due to https://github.com/OpenLineage/OpenLineage/issues/4280
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                flink_job=OpenLineageFlinkJobDetailsRunFacet(
                    jobId="b825f524-49d6-4dd8-bffd-3e5742c528d0",
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.19.0"),
                    name="flink",
                    openlineageAdapterVersion=Version("1.34.0"),
                ),
                # job tags are missing due to https://github.com/OpenLineage/OpenLineage/issues/4280
            ),
        ),
    )
    assert FlinkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="http", name="localhost:18081", addresses={"http://localhost:18081"}),
            type=JobTypeDTO(type="FLINK_JOB"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="flink.version"),
                    value="1.19.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.34.0",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        start_reason=None,
        user=None,
        ended_at=None,
        external_id="b825f524-49d6-4dd8-bffd-3e5742c528d0",
        attempt=None,
        persistent_log_url="http://localhost:18081/#/job/completed/b825f524-49d6-4dd8-bffd-3e5742c528d0",
        running_log_url="http://localhost:18081/#/job/running/b825f524-49d6-4dd8-bffd-3e5742c528d0",
    )


def test_extractors_extract_run_flink_before_openlineage_1_34():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
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
        ),
    )
    assert FlinkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="http", name="localhost:18081", addresses={"http://localhost:18081"}),
            type=JobTypeDTO(type="FLINK_JOB"),
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        start_reason=None,
        user=None,
        ended_at=None,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
