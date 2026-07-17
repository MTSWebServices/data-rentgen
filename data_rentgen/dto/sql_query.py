# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

from data_rentgen.utils.uuid import generate_static_uuid


@dataclass(slots=True)
class SQLQueryDTO:
    query: str
    fingerprint: UUID = field(init=False)
    id: int | None = field(default=None, compare=False)

    def __post_init__(self):
        self.fingerprint = generate_static_uuid(self.query)

    @property
    def unique_key(self):
        return self.fingerprint

    def merge(self, new: SQLQueryDTO) -> SQLQueryDTO:
        self.id = new.id or self.id
        return self
