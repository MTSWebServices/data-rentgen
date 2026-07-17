# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import IntFlag
from uuid import UUID

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.operation import OperationDTO
from data_rentgen.dto.schema import SchemaDTO
from data_rentgen.utils.uuid import generate_incremental_uuid


class OutputTypeDTO(IntFlag):
    UNKNOWN = 0

    APPEND = 1

    CREATE = 2
    ALTER = 4
    RENAME = 8

    OVERWRITE = 16

    DROP = 32
    TRUNCATE = 64

    DELETE = 128
    UPDATE = 256
    MERGE = 512


@dataclass(slots=True)
class OutputDTO:
    created_at: datetime
    operation: OperationDTO
    dataset: DatasetDTO
    type: OutputTypeDTO = OutputTypeDTO.UNKNOWN
    schema: SchemaDTO | None = None
    num_rows: int | None = None
    num_bytes: int | None = None
    num_files: int | None = None

    @property
    def unique_key(self):
        return (
            self.operation.unique_key,
            self.dataset.unique_key,
        )

    def generate_id(self) -> UUID:
        # Instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id.
        # This property could be accessed only after all nested ids are fetched from DB
        id_components = [
            str(self.operation.id),
            str(self.dataset.id),
            str(self.schema.id) if self.schema else "",
        ]
        return generate_incremental_uuid(self.created_at, ".".join(id_components))

    def merge(self, new: OutputDTO) -> OutputDTO:
        self.operation.merge(new.operation)
        self.dataset.merge(new.dataset)

        if new.schema:
            if self.schema:
                self.schema.merge(new.schema)
            else:
                self.schema = new.schema

        if not new.type.value & self.type.value:
            # IntFlag.__or__ is slow, avoid calling it if not needed
            self.type |= new.type

        self.created_at = min([new.created_at, self.created_at])
        self.num_rows = max(filter(None, [new.num_rows, self.num_rows]), default=None)
        self.num_bytes = max(filter(None, [new.num_bytes, self.num_bytes]), default=None)
        self.num_files = max(filter(None, [new.num_files, self.num_files]), default=None)
        return self
