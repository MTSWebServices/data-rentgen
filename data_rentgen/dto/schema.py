# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from dataclasses import dataclass, field
from uuid import UUID

from data_rentgen.utils.uuid import generate_static_uuid


@dataclass(slots=True)
class SchemaDTO:
    fields: list[dict]
    digest: UUID = field(init=False)
    id: int | None = field(default=None, compare=False)

    def __post_init__(self):
        self.digest = generate_static_uuid(json.dumps(self.fields, sort_keys=True))

    @property
    def unique_key(self):
        return self.digest

    def merge(self, new: SchemaDTO) -> SchemaDTO:
        self.id = new.id or self.id
        return self
