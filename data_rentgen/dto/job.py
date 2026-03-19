# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from copy import copy
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
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name.lower())

    def merge(self, new: JobDTO) -> JobDTO:
        self.id = new.id or self.id
        self.location = self.location.merge(new.location)
        if new.parent_job and self.parent_job:
            self.parent_job = self.parent_job.merge(new.parent_job)
        else:
            self.parent_job = new.parent_job or self.parent_job

        if new.type and self.type:
            self.type = self.type.merge(new.type)
        else:
            self.type = new.type or self.type

        self.tag_values.update(new.tag_values)

        if self.name == "unknown" and new.name != "unknown":
            # Workaround for https://github.com/OpenLineage/OpenLineage/issues/3846
            result = copy(self)
            result.name = new.name
            return result

        return self
