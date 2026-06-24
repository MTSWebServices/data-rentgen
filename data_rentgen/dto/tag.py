# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class TagDTO:
    name: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.name.lower(),)

    def __hash__(self):
        return hash(self.unique_key)

    def merge(self, new: TagDTO) -> TagDTO:
        self.id = new.id or self.id
        return self


@dataclass(slots=True)
class TagValueDTO:
    tag: TagDTO
    value: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.tag.unique_key, self.value.lower())

    def __hash__(self):
        return hash(self.unique_key)

    def merge(self, new: TagValueDTO) -> TagValueDTO:
        self.tag.merge(new.tag)
        self.id = new.id or self.id
        return self
