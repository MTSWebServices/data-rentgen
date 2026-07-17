# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag


class DatasetColumnRelationTypeDTO(IntFlag):
    # See https://github.com/OpenLineage/OpenLineage/blob/1.27.0/integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/plan/column/TransformationInfo.java#L30-L40
    # Using IntFlag to avoid messing up with ARRAY type, bitwise OR is enough
    UNKNOWN = 1

    # Direct
    IDENTITY = 2
    TRANSFORMATION = 4
    TRANSFORMATION_MASKING = 8
    AGGREGATION = 16
    AGGREGATION_MASKING = 32

    # Indirect
    FILTER = 64
    JOIN = 128
    GROUP_BY = 256
    SORT = 512
    WINDOW = 1024
    CONDITIONAL = 2048


@dataclass(slots=True)
class DatasetColumnRelationDTO:
    type: DatasetColumnRelationTypeDTO
    source_column: str
    target_column: str | None = None
    # id is generated using other ids combination

    @property
    def unique_key(self):
        return (
            self.source_column,
            self.target_column or "",
        )

    def merge(self, new: DatasetColumnRelationDTO) -> DatasetColumnRelationDTO:
        if new.type.value & self.type.value:
            # IntFlag.__or__ is slow, avoid calling it if not needed
            return self

        self.type |= new.type
        return self
