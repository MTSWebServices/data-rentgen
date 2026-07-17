# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class LocationDTO:
    type: str
    name: str
    addresses: set[str]
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self):
        return (self.type, self.name)

    def merge(self, new: LocationDTO) -> LocationDTO:
        self.addresses |= new.addresses
        self.id = new.id or self.id
        return self
