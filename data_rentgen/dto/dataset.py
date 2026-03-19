# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field

from data_rentgen.dto.location import LocationDTO
from data_rentgen.dto.tag import TagValueDTO


@dataclass(slots=True)
class DatasetDTO:
    location: LocationDTO
    name: str
    tag_values: set[TagValueDTO] = field(default_factory=set)
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.location.unique_key, self.name.lower())

    def merge(self, new: DatasetDTO) -> DatasetDTO:
        self.location.merge(new.location)
        self.id = new.id or self.id
        self.tag_values.update(new.tag_values)
        return self
