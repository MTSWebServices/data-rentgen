# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.job import JobDTO


@dataclass(slots=True)
class JobDependencyDTO:
    from_job: JobDTO
    to_job: JobDTO
    type: str | None = None
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.from_job.unique_key, self.to_job.unique_key, self.type)

    def merge(self, new: JobDependencyDTO) -> JobDependencyDTO:
        self.from_job = self.from_job.merge(new.from_job)
        self.to_job = self.to_job.merge(new.to_job)
        self.id = new.id or self.id
        return self
