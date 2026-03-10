from __future__ import annotations

from datetime import datetime, timezone

import pytest
from packaging.version import Version
from uuid6 import UUID

from data_rentgen.consumer.extractors.impl import AirflowDagExtractor, AirflowTaskExtractor
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
from data_rentgen.dto.job_dependency import JobDependencyDTO
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
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
    OpenLineageNominalTimeRunFacet,
    OpenLineageProcessingEngineRunFacet,
    OpenLineageRunFacets,
)
from data_rentgen.openlineage.run_facets.job_dependency import (
    OpenLineageJobDependenciesRunFacet,
    OpenLineageJobDependency,
    OpenLineageJobIdentifier,
    OpenLineageRunIdentifier,
)
from data_rentgen.openlineage.run_facets.run_tags import OpenLineageRunTagsFacet, OpenLineageRunTagsFacetField


def test_extractors_extract_run_airflow_dag_log_url_3_x_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.0.1"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.7.3"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
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

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="3.0.1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.7.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value="1.38.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=("http://airflow-host:8081/dags/mydag/runs/manual__2024-07-05T09:04:13:979349+00:00"),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_dag_log_url_2_3_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.10.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_dag_log_url_2_x():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.1.4"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.1.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.10.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=(
            "http://airflow-host:8081/graph?dag_id=mydag&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_log_url_preserve_original():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        log_url=(
                            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
                        ),
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.10.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_log_url_3_x():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.0.1"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.7.3"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="backfill__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.BACKFILL_JOB,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
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

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="3.0.1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.7.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value="1.38.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="backfill__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/runs/backfill__2024-07-05T09:04:13:979349+00:00/tasks/mytask?try_number=1"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_log_url_2_9_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("1.9.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="backfill__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.BACKFILL_JOB,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.9.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="backfill__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=backfill__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_log_url_2_x():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0e9b-7c52-9856-6cecfc842610")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["owner", "extracted_user"],
    [
        ("myuser", UserDTO(name="myuser")),
        (None, None),
        ("airflow", None),
        ("***", None),
    ],
)
def test_extractors_extract_run_airflow_dag_owner(owner: str, extracted_user: UserDTO | None):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.1.4"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("1.10.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner=owner),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.1.4",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="1.10.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=extracted_user,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=(
            "http://airflow-host:8081/graph?dag_id=mydag&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["owner", "extracted_user"],
    [
        ("myuser", UserDTO(name="myuser")),
        (None, None),
        ("airflow", None),
        ("***", None),
    ],
)
def test_extractors_extract_run_airflow_task_owner(owner: str, extracted_user: UserDTO | None):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner=owner),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=extracted_user,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_3_x_task_map_index():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask_10",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("3.0.1"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.7.3"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        map_index=10,
                    ),
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

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask_10",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="3.0.1",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.7.3",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_client.version"),
                    value="1.38.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/runs/scheduled__2024-07-05T09:04:13:979349+00:00/tasks/mytask/mapped/10?try_number=1"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_2_x_task_map_index():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask_10",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                        map_index=10,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask_10",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/log?&dag_id=mydag&task_id=mytask&execution_date=2024-07-05T09%3A04%3A13.979349%2B00%3A00"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_dag_tags_2_10_x_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
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
                        OpenLineageJobTagsFacetField(
                            key="from_dag.prod",
                            value="from_dag.prod",
                            source="AIRFLOW",
                        ),
                        OpenLineageJobTagsFacetField(
                            key="from_dag.environment:production",
                            value="from_dag.environment:production",
                            source="AIRFLOW",
                        ),
                    ],
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.10.0"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.4.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
                tags=OpenLineageRunTagsFacet(
                    tags=[
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
                    ],
                ),
            ),
        ),
    )

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.10.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.4.0",
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
                # no from_dag.prod
                TagValueDTO(
                    tag=TagDTO(name="from_dag.environment"),
                    value="production",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    "tags",
    [
        [
            "prod",
            "master",
            "feature/ABC-123",
            "environment:production",
        ],
        ('["prod","master","feature/ABC-123","environment:production"]'),
        ("['prod','master','feature/ABC-123','environment:production']"),
    ],
)
def test_extractors_extract_run_airflow_dag_tags_below_2_10(tags: list[str] | str):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="DAG",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.3.0"),
                ),
                airflowDagRun=OpenLineageAirflowDagRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow", tags=tags),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="manual__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.MANUAL,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                ),
            ),
        ),
    )

    assert AirflowDagExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_DAG"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.3.0",
                ),
                # no other tags as they are not in key::value form
                TagValueDTO(
                    tag=TagDTO(name="environment"),
                    value="production",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.MANUAL,
        user=None,
        ended_at=now,
        external_id="manual__2024-07-05T09:04:13:979349+00:00",
        attempt=None,
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?dag_run_id=manual__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00"
        ),
        running_log_url=None,
    )


def test_extractors_extract_run_airflow_task_tags_2_10_x_plus():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
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
                        OpenLineageJobTagsFacetField(
                            key="from_dag.prod",
                            value="from_dag.prod",
                            source="AIRFLOW",
                        ),
                        OpenLineageJobTagsFacetField(
                            key="from_dag.environment:production",
                            value="from_dag.environment:production",
                            source="AIRFLOW",
                        ),
                    ],
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.10.0"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.4.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
                tags=OpenLineageRunTagsFacet(
                    tags=[
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
                    ],
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.10.0",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.4.0",
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
                # no from_dag.prod
                TagValueDTO(
                    tag=TagDTO(name="from_dag.environment"),
                    value="production",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=scheduled__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    "tags",
    [
        [
            "prod",
            "master",
            "feature/ABC-123",
            "environment:production",
        ],
        ('["prod","master","feature/ABC-123","environment:production"]'),
        ("['prod','master','feature/ABC-123','environment:production']"),
    ],
)
def test_extractors_extract_run_airflow_task_tags_below_2_10(tags: list[str] | str):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.3.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow", tags=tags),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.3.0",
                ),
                # no other tags as they are not in key::value form
                TagValueDTO(
                    tag=TagDTO(name="environment"),
                    value="production",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=scheduled__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
    )


@pytest.mark.parametrize(
    ["start_time", "end_time", "expected_start_at", "expected_end_at"],
    [
        (
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            None,
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            None,
        ),
        (
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            None,
        ),
        (
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            datetime(2024, 7, 5, 9, 4, 31, 979349, tzinfo=timezone.utc),
            datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
            datetime(2024, 7, 5, 9, 4, 31, 979349, tzinfo=timezone.utc),
        ),
    ],
)
def test_extractors_extract_run_airflow_task_nominal_times(
    start_time: datetime | None,
    end_time: datetime | None,
    expected_start_at: datetime | None,
    expected_end_at: datetime | None,
):
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.3.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
                nominalTime=OpenLineageNominalTimeRunFacet(
                    nominalStartTime=start_time,
                    nominalEndTime=end_time,
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.3.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=scheduled__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
        expected_start_at=expected_start_at,
        expected_end_at=expected_end_at,
    )


def test_extractors_extract_run_airflow_task_job_dependencies_ol_provider_2_10_plus():
    # https://github.com/apache/airflow/pull/59521
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.3.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
                jobDependencies=OpenLineageJobDependenciesRunFacet(
                    upstream=[
                        OpenLineageJobDependency(
                            job=OpenLineageJobIdentifier(
                                namespace="http://airflow-host:8081",
                                name="mydag.previous_task",
                            ),
                            run=OpenLineageRunIdentifier(
                                runId=UUID("01908223-0782-79b8-9495-b1c38aaee831"),
                            ),
                            dependency_type="IMPLICIT_DEPENDENCY",
                            sequence_trigger_rule="FINISH_TO_START",
                            status_trigger_rule="EXECUTE_EVERY_TIME",
                        ),
                    ],
                    downstream=[
                        OpenLineageJobDependency(
                            job=OpenLineageJobIdentifier(
                                namespace="http://airflow-host:8081",
                                name="mydag.next_task",
                            ),
                            dependency_type="IMPLICIT_DEPENDENCY",
                            sequence_trigger_rule="FINISH_TO_START",
                            status_trigger_rule="EXECUTE_EVERY_TIME",
                        ),
                    ],
                    trigger_rule="ALL_SUCCESS",
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.3.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=scheduled__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
        job_dependencies=[
            JobDependencyDTO(
                from_job=JobDTO(
                    name="mydag.previous_task",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                ),
                to_job=JobDTO(
                    name="mydag.mytask",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                    tag_values={
                        TagValueDTO(
                            tag=TagDTO(name="airflow.version"),
                            value="2.9.2",
                        ),
                        TagValueDTO(
                            tag=TagDTO(name="openlineage_adapter.version"),
                            value="2.3.0",
                        ),
                    },
                ),
                type="IMPLICIT_DEPENDENCY",
            ),
            JobDependencyDTO(
                from_job=JobDTO(
                    name="mydag.mytask",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                    tag_values={
                        TagValueDTO(
                            tag=TagDTO(name="airflow.version"),
                            value="2.9.2",
                        ),
                        TagValueDTO(
                            tag=TagDTO(name="openlineage_adapter.version"),
                            value="2.3.0",
                        ),
                    },
                ),
                to_job=JobDTO(
                    name="mydag.next_task",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                ),
                type="IMPLICIT_DEPENDENCY",
            ),
        ],
    )


def test_extractors_extract_run_airflow_task_job_dependencies_ol_provider_below_2_10():
    now = datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc)
    run_id = UUID("01908223-0782-79b8-9495-b1c38aaee839")
    run = OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.COMPLETE,
        eventTime=now,
        job=OpenLineageJob(
            namespace="http://airflow-host:8081",
            name="mydag.mytask",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="AIRFLOW",
                    jobType="TASK",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=run_id,
            facets=OpenLineageRunFacets(
                processing_engine=OpenLineageProcessingEngineRunFacet(
                    version=Version("2.9.2"),
                    name="Airflow",
                    openlineageAdapterVersion=Version("2.3.0"),
                ),
                airflow=OpenLineageAirflowTaskRunFacet(
                    dag=OpenLineageAirflowDagInfo(dag_id="mydag", owner="airflow"),
                    dagRun=OpenLineageAirflowDagRunInfo(
                        run_id="scheduled__2024-07-05T09:04:13:979349+00:00",
                        run_type=OpenLineageAirflowDagRunType.SCHEDULED,
                        data_interval_start=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                        data_interval_end=datetime(2024, 7, 5, 9, 4, 13, 979349, tzinfo=timezone.utc),
                    ),
                    task=OpenLineageAirflowTaskInfo(
                        task_id="mytask",
                        operator_class="BashOperator",
                        downstream_task_ids=["next_task"],
                        upstream_task_ids=["previous_task"],
                    ),
                    taskInstance=OpenLineageAirflowTaskInstanceInfo(
                        try_number=1,
                    ),
                ),
            ),
        ),
    )

    assert AirflowTaskExtractor().extract_run(run) == RunDTO(
        id=run_id,
        job=JobDTO(
            name="mydag.mytask",
            location=LocationDTO(
                type="http",
                name="airflow-host:8081",
                addresses={"http://airflow-host:8081"},
            ),
            type=JobTypeDTO(type="AIRFLOW_TASK"),
            tag_values={
                TagValueDTO(
                    tag=TagDTO(name="airflow.version"),
                    value="2.9.2",
                ),
                TagValueDTO(
                    tag=TagDTO(name="openlineage_adapter.version"),
                    value="2.3.0",
                ),
            },
        ),
        status=RunStatusDTO.SUCCEEDED,
        started_at=None,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        user=None,
        ended_at=now,
        external_id="scheduled__2024-07-05T09:04:13:979349+00:00",
        attempt="1",
        persistent_log_url=(
            "http://airflow-host:8081/dags/mydag/grid?tab=logs&dag_run_id=scheduled__2024-07-05T09%3A04%3A13%3A979349%2B00%3A00&task_id=mytask&map_index=-1"
        ),
        running_log_url=None,
        job_dependencies=[
            JobDependencyDTO(
                from_job=JobDTO(
                    name="mydag.previous_task",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                ),
                to_job=JobDTO(
                    name="mydag.mytask",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                    tag_values={
                        TagValueDTO(
                            tag=TagDTO(name="airflow.version"),
                            value="2.9.2",
                        ),
                        TagValueDTO(
                            tag=TagDTO(name="openlineage_adapter.version"),
                            value="2.3.0",
                        ),
                    },
                ),
                type="DIRECT_DEPENDENCY",
            ),
            JobDependencyDTO(
                from_job=JobDTO(
                    name="mydag.mytask",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                    tag_values={
                        TagValueDTO(
                            tag=TagDTO(name="airflow.version"),
                            value="2.9.2",
                        ),
                        TagValueDTO(
                            tag=TagDTO(name="openlineage_adapter.version"),
                            value="2.3.0",
                        ),
                    },
                ),
                to_job=JobDTO(
                    name="mydag.next_task",
                    location=LocationDTO(
                        type="http",
                        name="airflow-host:8081",
                        addresses={"http://airflow-host:8081"},
                    ),
                    type=JobTypeDTO(type="AIRFLOW_TASK"),
                ),
                type="DIRECT_DEPENDENCY",
            ),
        ],
    )
