from __future__ import annotations

from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import DbtExtractor
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
    OpenLineageJobTagsFacet,
    OpenLineageJobTagsFacetField,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import OpenLineageDbtRunRunFacet, OpenLineageRunFacets
from data_rentgen.openlineage.run_facets.processing_engine import OpenLineageProcessingEngineRunFacet
from data_rentgen.openlineage.run_facets.run_tags import OpenLineageRunTagsFacet, OpenLineageRunTagsFacetField


def test_extractors_extract_run_dbt():
    now = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.9.4"),
                    name="dbt",
                    openlineageAdapterVersion=Version("1.33.0"),
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="dbt-run-demo_project",
            location=LocationDTO(
                type="local",
                name="somehost",
                addresses={"local://somehost"},
            ),
            type=JobTypeDTO(type="DBT_JOB"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="dbt.version"),
                    value="1.9.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.33.0",
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


def test_extractors_extract_run_dbt_openlineage_1_34_plus():
    # https://github.com/OpenLineage/OpenLineage/pull/3738
    now = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(
                    invocation_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.9.4"),
                    name="dbt",
                    openlineageAdapterVersion=Version("1.34.0"),
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="dbt-run-demo_project",
            location=LocationDTO(
                type="local",
                name="somehost",
                addresses={"local://somehost"},
            ),
            type=JobTypeDTO(type="DBT_JOB"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="dbt.version"),
                    value="1.9.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.34.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=now,
        external_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


def test_extractors_extract_run_dbt_openlineage_1_38_plus():
    # https://github.com/OpenLineage/OpenLineage/pull/3980
    now = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(
                    invocation_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.9.4"),
                    name="dbt",
                    openlineageAdapterVersion=Version("1.38.0"),
                ),
                tags=OpenLineageRunTagsFacet(
                    tags=[
                        OpenLineageRunTagsFacetField(
                            key="openlineage_client_version",
                            value="1.38.0",
                            source="OPENLINEAGE_CLIENT",
                        ),
                    ],
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="dbt-run-demo_project",
            location=LocationDTO(
                type="local",
                name="somehost",
                addresses={"local://somehost"},
            ),
            type=JobTypeDTO(type="DBT_JOB"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="dbt.version"),
                    value="1.9.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.38.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value="1.38.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=now,
        external_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["model_tag", "expected_tag_value"],
    [
        (
            "from_model.tag1",
            TagValueDTO(
                tag=TagDTO(name="from_model.tag1"),
                value="true",
            ),
        ),
        (
            "from_model.tag2:value2",
            TagValueDTO(
                tag=TagDTO(name="from_model.tag2"),
                value="value2",
            ),
        ),
    ],
)
def test_extractors_extract_run_dbt_openlineage_with_tags(model_tag: str, expected_tag_value: TagValueDTO):
    # https://github.com/OpenLineage/OpenLineage/pull/3471
    # https://github.com/OpenLineage/OpenLineage/pull/4022
    now = datetime(2025, 5, 20, 8, 27, 20, 413075, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventTime=now,
        eventType=OpenLineageRunEventType.COMPLETE,
        job=OpenLineageJob(
            namespace="local://somehost",
            name="dbt-run-demo_project",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="DBT",
                    jobType="JOB",
                ),
                tags=OpenLineageJobTagsFacet(
                    tags=[
                        OpenLineageJobTagsFacetField(
                            key="from_job_config",
                            value="somevalue1",
                            source="USER",
                        ),
                        OpenLineageJobTagsFacetField(
                            key="from_job_config",
                            value="anothervalue1",
                            source="CUSTOM",
                        ),
                    ],
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                dbt_run=OpenLineageDbtRunRunFacet(
                    invocation_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
                ),
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("1.9.4"),
                    name="dbt",
                    openlineageAdapterVersion=Version("1.38.0"),
                ),
                tags=OpenLineageRunTagsFacet(
                    tags=[
                        OpenLineageRunTagsFacetField(
                            key="openlineage_client_version",
                            value="1.38.0",
                            source="OPENLINEAGE_CLIENT",
                        ),
                        OpenLineageRunTagsFacetField(
                            key="from_run_config",
                            value="somevalue2",
                            source="CONFIG",
                        ),
                        OpenLineageRunTagsFacetField(
                            key="from_run_config",
                            value="anothervalue2",
                            source="CUSTOM",
                        ),
                        OpenLineageRunTagsFacetField(
                            key=model_tag,
                            value="true",
                            source="DBT",
                        ),
                    ],
                ),
            ),
        ),
    )

    assert DbtExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="dbt-run-demo_project",
            location=LocationDTO(
                type="local",
                name="somehost",
                addresses={"local://somehost"},
            ),
            type=JobTypeDTO(type="DBT_JOB"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="dbt.version"),
                    value="1.9.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.38.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value="1.38.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from_job_config"),
                    value="somevalue1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from_job_config"),
                    value="anothervalue1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from_run_config"),
                    value="somevalue2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from_run_config"),
                    value="anothervalue2",
                ),
                expected_tag_value,
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=None,
        user=None,
        ended_at=now,
        external_id="93c69fcd-10d0-4639-a4f8-95be0da4476b",
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )
