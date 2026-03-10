# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, IntEnum
from uuid import UUID

from data_rentgen.dto.job import JobDTO
from data_rentgen.dto.job_dependency import JobDependencyDTO
from data_rentgen.dto.user import UserDTO
from data_rentgen.utils.uuid import extract_timestamp_from_uuid


class RunStatusDTO(IntEnum):
    UNKNOWN = -1
    STARTED = 0
    SUCCEEDED = 1
    FAILED = 2
    KILLED = 3


class RunStartReasonDTO(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"

    def __str__(self) -> str:
        return str(self.value)


@dataclass(slots=True)
class RunDTO:
    id: UUID
    created_at: datetime = field(init=False)
    job: JobDTO
    parent_run: RunDTO | None = None
    status: RunStatusDTO = RunStatusDTO.UNKNOWN
    started_at: datetime | None = None
    start_reason: RunStartReasonDTO | None = None
    user: UserDTO | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    attempt: str | None = None
    running_log_url: str | None = None
    persistent_log_url: str | None = None
    expected_start_at: datetime | None = None
    expected_end_at: datetime | None = None
    job_dependencies: list[JobDependencyDTO] = field(default_factory=list)

    def __post_init__(self):
        self.created_at = extract_timestamp_from_uuid(self.id)

    @property
    def unique_key(self) -> tuple:
        return (self.id,)

    def merge(self, new: RunDTO) -> RunDTO:
        self.job = self.job.merge(new.job)
        if new.parent_run and self.parent_run:
            self.parent_run = self.parent_run.merge(new.parent_run)
        else:
            self.parent_run = new.parent_run or self.parent_run

        if new.user and self.user:
            self.user = self.user.merge(new.user)
        else:
            self.user = new.user or self.user

        existing_dependencies = {item.unique_key: item for item in self.job_dependencies}
        merged_dependencies = []
        for job_dependency in new.job_dependencies:
            if job_dependency.unique_key in existing_dependencies:
                merged_dependencies.append(existing_dependencies[job_dependency.unique_key].merge(job_dependency))
            else:
                merged_dependencies.append(job_dependency)
        self.job_dependencies = merged_dependencies

        self.status = max(new.status, self.status)
        self.started_at = new.started_at or self.started_at
        self.start_reason = new.start_reason or self.start_reason
        self.ended_at = new.ended_at or self.ended_at
        self.external_id = new.external_id or self.external_id
        self.attempt = new.attempt or self.attempt
        self.running_log_url = new.running_log_url or self.running_log_url
        self.persistent_log_url = new.persistent_log_url or self.persistent_log_url
        self.expected_start_at = new.expected_start_at or self.expected_start_at
        self.expected_end_at = new.expected_end_at or self.expected_end_at
        return self
