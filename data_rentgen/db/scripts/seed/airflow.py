# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime, timedelta

from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    RunDTO,
    RunStartReasonDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.utils.uuid import generate_new_uuid


def generate_airflow_run(dag_id: str, task_id: str, created_at: datetime, ended_at: datetime) -> RunDTO:
    address = "https://airflow01.companyname.com"
    location = LocationDTO(
        type="https",
        name="airflow01.companyname.com",
        addresses={address},
    )

    dag_job = JobDTO(
        name=dag_id,
        location=location,
        type=JobTypeDTO(type="AIRFLOW_DAG"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="airflow.version"),
                value="2.11.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="2.10.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_client.version"),
                value="1.43.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="environment"),
                value="production",
            ),
        },
    )
    dag_run_id = f"scheduled__{created_at.isoformat()}"
    dag_ui = address + f"/dags/{dag_job.name}/runs/{dag_run_id}"
    dag_run = RunDTO(
        id=generate_new_uuid(created_at),
        job=dag_job,
        started_at=created_at,
        ended_at=ended_at,
        external_id=dag_run_id,
        persistent_log_url=dag_ui,
        status=RunStatusDTO.SUCCEEDED,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        expected_start_at=created_at.replace(minute=(created_at.minute // 5) * 5, second=0, microsecond=0),
        expected_end_at=ended_at.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1),
    )

    task_job = JobDTO(
        name=f"{dag_id}.{task_id}",
        location=location,
        parent_job=dag_job,
        type=JobTypeDTO(type="AIRFLOW_TASK"),
        tag_values={
            TagValueDTO(
                tag=TagDTO(name="airflow.version"),
                value="2.11.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value="2.10.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="openlineage_client.version"),
                value="1.43.0",
            ),
            TagValueDTO(
                tag=TagDTO(name="environment"),
                value="production",
            ),
        },
    )
    task_created_at = created_at + timedelta(seconds=1)
    task_ended_at = ended_at - timedelta(seconds=1)
    return RunDTO(
        id=generate_new_uuid(task_created_at),
        job=task_job,
        parent_run=dag_run,
        started_at=task_created_at,
        ended_at=task_ended_at,
        external_id=dag_run_id,
        attempt="1",
        persistent_log_url=dag_ui + f"/tasks/{task_job.name}?try_number=1",
        status=RunStatusDTO.SUCCEEDED,
        start_reason=RunStartReasonDTO.AUTOMATIC,
        expected_start_at=dag_run.expected_start_at,
        expected_end_at=dag_run.expected_end_at,
    )
