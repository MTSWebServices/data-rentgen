# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.utils.uuid import generate_static_uuid


class DatasetSymlinkTypeDTO(str, Enum):
    METASTORE = "METASTORE"
    WAREHOUSE = "WAREHOUSE"

    def __str__(self) -> str:
        return self.value


DatasetSymlinkMemberDTO = tuple[DatasetDTO, DatasetSymlinkTypeDTO]


def compute_symlink_fingerprint(
    members: Iterable[DatasetSymlinkMemberDTO],
) -> UUID:
    normalized = sorted((dataset.unique_key, str(role)) for dataset, role in members)
    return generate_static_uuid(
        json.dumps(normalized, ensure_ascii=True),
    )


@dataclass(slots=True)
class DatasetSymlinkGroupDTO:
    members: list[DatasetSymlinkMemberDTO]

    @property
    def fingerprint(self) -> UUID:
        return compute_symlink_fingerprint(self.members)

    @property
    def unique_key(self) -> UUID:
        return self.fingerprint

    def merge(self, new: DatasetSymlinkGroupDTO) -> DatasetSymlinkGroupDTO:
        return self
