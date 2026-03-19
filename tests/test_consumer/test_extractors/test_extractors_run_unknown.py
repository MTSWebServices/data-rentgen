from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import UnknownExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
    UserDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTagsFacet,
    OpenLineageJobTagsFacetField,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import DataRentgenRunInfoFacet, OpenLineageRunFacets
from data_rentgen.openlineage.run_facets.parent_run import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
)
from data_rentgen.openlineage.run_facets.run_tags import OpenLineageRunTagsFacet, OpenLineageRunTagsFacetField


@pytest.mark.parametrize(
    ["raw_job_type", "extracted_job_type"],
    [
        (None, None),
        (
            OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="ABC",
            ),
            JobTypeDTO(type="ABC"),
        ),
        (
            OpenLineageJobTypeJobFacet(
                processingType=OpenLineageJobProcessingType.NONE,
                integration="ABC",
                jobType="CDE",
            ),
            JobTypeDTO(type="ABC_CDE"),
        ),
    ],
)
def test_extractors_extract_run_unknown(
    raw_job_type: OpenLineageJobTypeJobFacet | None,
    extracted_job_type: JobTypeDTO | None,
):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="something",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=raw_job_type,
                tags=OpenLineageJobTagsFacet(
                    tags=[
                        OpenLineageJobTagsFacetField(
                            key="from.job.config",
                            value="somevalue1",
                            source="CONFIG",  # ignored
                        ),
                        OpenLineageJobTagsFacetField(
                            key="from.job.config",
                            value="othervalue1",
                            source="USER",  # ignored
                        ),
                        OpenLineageJobTagsFacetField(
                            key="from.job.config",
                            value="anothervalue1",
                            source="CUSTOM",  # ignored
                        ),
                    ],
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                tags=OpenLineageRunTagsFacet(
                    tags=[
                        OpenLineageRunTagsFacetField(
                            key="from.run.config",
                            value="somevalue2",
                            source="CONFIG",  # ignored
                        ),
                        OpenLineageRunTagsFacetField(
                            key="from.run.config",
                            value="othervalue2",
                            source="USER",  # ignored
                        ),
                        OpenLineageRunTagsFacetField(
                            key="from.run.config",
                            value="anothervalue2",
                            source="CUSTOM",  # ignored
                        ),
                    ],
                ),
            ),
        ),
    )

    assert UnknownExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            type=extracted_job_type,
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="from.job.config"),
                    value="somevalue1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.job.config"),
                    value="othervalue1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.job.config"),
                    value="anothervalue1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.run.config"),
                    value="somevalue2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.run.config"),
                    value="othervalue2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.run.config"),
                    value="anothervalue2",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=now,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["event_type", "expected_status"],
    [
        (OpenLineageRunEventType.FAIL, RunStatusDTO.FAILED),
        (OpenLineageRunEventType.ABORT, RunStatusDTO.KILLED),
        (OpenLineageRunEventType.OTHER, RunStatusDTO.UNKNOWN),
    ],
)
def test_extractors_extract_run_with_status(
    event_type: OpenLineageRunEventType,
    expected_status: RunStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    run = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(namespace="something", name="myjob"),
        run=OpenLineageRun(runId=run_id),
    )

    ended_at = now if expected_status != RunStatusDTO.UNKNOWN else None
    assert UnknownExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
        ),
        status=expected_status,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=ended_at,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


def test_extractors_extract_run_unknown_with_parent():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    parent_run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="something",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="UNKNOWN_SOMETHING",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="parentjob",
                    ),
                    run=OpenLineageParentRun(
                        runId=parent_run_id,
                    ),
                ),
            ),
        ),
    )

    assert UnknownExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            parent_job=JobDTO(
                name="parentjob",
                type=None,
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
            type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
        ),
        parent_run=RunDTO(
            id=parent_run_id,
            job=JobDTO(
                name="parentjob",
                type=None,
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
            status=RunStatusDTO.UNKNOWN,
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )


def test_extractors_extract_run_unknown_with_custom_run_info():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="something",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="UNKNOWN_SOMETHING",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dataRentgen_run=DataRentgenRunInfoFacet(
                    external_id="external_id",
                    attempt="attempt",
                    running_log_url="running_log_url",
                    persistent_log_url="persistent_log_url",
                    start_reason="AUTOMATIC",
                    started_by_user="someuser",
                ),
            ),
        ),
    )

    assert UnknownExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
            location=LocationDTO(
                type="unknown",
                name="something",
                addresses={"unknown://something"},
            ),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        ended_at=now,
        external_id="external_id",
        attempt="attempt",
        persistent_log_url="persistent_log_url",
        running_log_url="running_log_url",
        user=UserDTO(name="someuser"),
    )
