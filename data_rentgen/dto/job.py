# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.job_type import JobTypeDTO
from data_rentgen.dto.location import LocationDTO
from data_rentgen.dto.tag import TagValueDTO


@dataclass(slots=True)
class JobDTO:
    name: str
    location: LocationDTO
    parent_job: JobDTO | None = None
    type: JobTypeDTO | None = None
    tag_values: set[TagValueDTO] = field(default_factory=set)
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self):
        return (self.location.unique_key, self.name.lower())

    def merge(self, new: JobDTO) -> JobDTO:
        self.id = new.id or self.id
        self.location.merge(new.location)

        # Workaround for https://github.com/OpenLineage/OpenLineage/issues/3846
        if new.parent_job:
            if self.parent_job and self.parent_job.unique_key == new.parent_job.unique_key:
                self.parent_job.merge(new.parent_job)
            else:
                self.parent_job = new.parent_job

        if new.type:
            if self.type and self.type.unique_key == new.type.unique_key:
                self.type.merge(new.type)
            else:
                self.type = new.type

        self.tag_values.update(new.tag_values)
        return self
