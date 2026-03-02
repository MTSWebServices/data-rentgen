from __future__ import annotations

from datetime import datetime, timezone

from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import SparkExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
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
from data_rentgen.openlineage.run_facets import (
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
    OpenLineageRunTagsFacet,
    OpenLineageRunTagsFacetField,
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.openlineage.run_facets.parent_run import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
)


def test_extractors_extract_run_spark_app_yarn():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.START,
        eventTime=now,
        job=OpenLineageJob(
            namespace="yarn://cluster",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
                ),
                tags=OpenLineageJobTagsFacet(
                    tags=[
                        OpenLineageJobTagsFacetField(
                            key="from.job.config",
                            value="somevalue1",
                            source="CONFIG",  # ignored
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
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                    openlineageAdapterVersion=Version("1.19.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="yarn",
                    appName="myapp",
                    applicationId="application_1234_5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040",
                    proxyUrl="http://yarn-proxy:8088/proxy/application_1234_5678,http://yarn-proxy:18088/proxy/application_1234_5678",
                    historyUrl="http://history-server:18080/history/application_1234_5678,http://history-server:18081/history/application_1234_5678",
                ),
                tags=OpenLineageRunTagsFacet(
                    tags=[
                        OpenLineageRunTagsFacetField(
                            key="from.run.config",
                            value="somevalue2",
                            source="CONFIG",  # ignored
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
    assert SparkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="yarn", name="cluster", addresses={"yarn://cluster"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.19.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="from.job.config"),
                    value="somevalue1",
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
                    value="anothervalue2",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=now,
        start_reason=None,
        user=UserDTO(name="myuser"),
        ended_at=None,
        external_id="application_1234_5678",
        attempt=None,
        persistent_log_url="http://history-server:18080/history/application_1234_5678",
        running_log_url="http://yarn-proxy:8088/proxy/application_1234_5678",
    )


def test_extractors_extract_run_spark_app_local():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="host://some.host.com",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                    openlineageAdapterVersion=Version("1.19.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[4]",
                    appName="myapp",
                    applicationId="local-1234-5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040,http://localhost:4041",
                ),
            ),
        ),
    )

    assert SparkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.19.0",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=None,
        start_reason=None,
        user=UserDTO(name="myuser"),
        external_id="local-1234-5678",
        attempt=None,
        persistent_log_url=None,
        running_log_url="http://localhost:4040",
    )


def test_extractors_extract_run_spark_app_no_openlineage_adapter_version():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="host://some.host.com",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                ),
            ),
        ),
    )

    assert SparkExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="myjob",
            location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
            },
        ),
        status=RunStatusDTO.STARTED,
        started_at=None,
        start_reason=None,
        user=None,
        external_id=None,
        attempt=None,
        persistent_log_url=None,
        running_log_url=None,
    )


def test_extractors_extract_run_spark_parent_job():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    parent_run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=now,
        job=OpenLineageJob(
            namespace="host://some.host.com",
            name="myjob",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.NONE,
                    integration="SPARK",
                    jobType="APPLICATION",
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
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.4.3"),
                    name="spark",
                    openlineageAdapterVersion=Version("1.19.0"),
                ),
                spark_applicationDetails=OpenLineageSparkApplicationDetailsRunFacet(
                    master="local[4]",
                    appName="myapp",
                    applicationId="local-1234-5678",
                    deployMode=OpenLineageSparkDeployMode.CLIENT,
                    driverHost="localhost",
                    userName="myuser",
                    uiWebUrl="http://localhost:4040,http://localhost:4041",
                ),
            ),
        ),
    )

    assert SparkExtractor().extract_run(run) == RunDTO(
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
            location=LocationDTO(type="host", name="some.host.com", addresses={"host://some.host.com"}),
            type=JobTypeDTO(type="SPARK_APPLICATION"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="spark.version"),
                    value="3.4.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.19.0",
                ),
            },
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
        status=RunStatusDTO.STARTED,
        started_at=None,
        start_reason=None,
        user=UserDTO(name="myuser"),
        external_id="local-1234-5678",
        attempt=None,
        persistent_log_url=None,
        running_log_url="http://localhost:4040",
    )
