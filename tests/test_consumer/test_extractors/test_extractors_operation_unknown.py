from __future__ import annotations

from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import UnknownExtractor
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
from data_rentgen.dto.run import RunStartReasonDTO
from data_rentgen.dto.user import UserDTO
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
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)
from data_rentgen.openlineage.run_facets.data_rentgen_operation import DataRentgenOperationInfoFacet
from data_rentgen.openlineage.run_facets.data_rentgen_run import DataRentgenRunInfoFacet


def test_extractors_extract_operation_unknown():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    parent_run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")

    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(namespace="anything", name="somejob"),
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
    assert UnknownExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="somejob",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
                parent_job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            parent_run=RunDTO(
                id=parent_run_id,
                job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            status=RunStatusDTO.STARTED,
            started_at=now,
            ended_at=None,
        ),
        name="somejob",
        type=OperationTypeDTO.BATCH,
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
def test_extractors_extract_operation_unknown_finished(
    event_type: OpenLineageRunEventType,
    expected_status: OperationStatusDTO,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    parent_run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation = OpenLineageRunEvent(
        eventType=event_type,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="somejob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="UNKNOWN",
                    jobType="SOMETHING",
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

    ended_at = now if expected_status != OperationStatusDTO.UNKNOWN else None

    assert UnknownExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="somejob",
                type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
                parent_job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            parent_run=RunDTO(
                id=parent_run_id,
                job=JobDTO(
                    name="parentjob",
                    location=LocationDTO(
                        type="unknown",
                        name="anything",
                        addresses={"unknown://anything"},
                    ),
                ),
            ),
            status=RunStatusDTO(expected_status),
            started_at=None,
            ended_at=ended_at,
        ),
        name="somejob",
        type=OperationTypeDTO.BATCH,
        position=None,
        description=None,
        status=expected_status,
        started_at=None,
        ended_at=ended_at,
    )


def test_extractors_extract_operation_unknown_with_explicit_operation_info():
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    operation_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="someoperation",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="UNKNOWN",
                    jobType="SOMETHING",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="anything",
                        name="myjob",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
                dataRentgen_operation=DataRentgenOperationInfoFacet(
                    description="some description",
                    group="some group",
                    position=1,
                ),
            ),
        ),
    )

    assert UnknownExtractor().extract_operation(operation) == OperationDTO(
        id=operation_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
                ),
            ),
            status=RunStatusDTO.UNKNOWN,
        ),
        name="someoperation",
        type=OperationTypeDTO.BATCH,
        position=1,
        description="some description",
        group="some group",
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )


@pytest.mark.parametrize(
    ["custom_operation_name", "expected_operation_name"],
    [
        (None, "myjob"),
        ("custom_operation_name", "custom_operation_name"),
    ],
)
def test_extractors_extract_operation_unknown_with_explicit_operation_and_run_info(
    custom_operation_name: str | None,
    expected_operation_name: str,
):
    now = datetime(2024, 7, 5, 9, 6, 29, 462000, tzinfo=timezone.utc)
    run_id = UUID("01908225-1fd7-746b-910C-70d24f2898b1")
    operation = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="anything",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="UNKNOWN",
                    jobType="SOMETHING",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dataRentgen_operation=DataRentgenOperationInfoFacet(
                    name=custom_operation_name,
                    description="some description",
                    group="some group",
                    position=1,
                ),
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

    assert UnknownExtractor().extract_operation(operation) == OperationDTO(
        id=run_id,
        run=RunDTO(
            id=run_id,
            job=JobDTO(
                name="myjob",
                type=JobTypeDTO(type="UNKNOWN_SOMETHING"),
                location=LocationDTO(
                    type="unknown",
                    name="anything",
                    addresses={"unknown://anything"},
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
        ),
        name=expected_operation_name,
        type=OperationTypeDTO.BATCH,
        position=1,
        description="some description",
        group="some group",
        status=OperationStatusDTO.SUCCEEDED,
        started_at=None,
        ended_at=now,
    )
