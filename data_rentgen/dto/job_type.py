# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class JobTypeDTO:
    type: str
    id: int | None = field(default=None, compare=False)

    @property
    def unique_key(self) -> tuple:
        return (self.type,)

    def merge(self, new: JobTypeDTO) -> JobTypeDTO:
        self.id = new.id or self.id
        return self
