# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime
from functools import cached_property
from uuid import UUID

from data_rentgen.dto.dataset import DatasetDTO
from data_rentgen.dto.dataset_column_relation import (
    DatasetColumnRelationDTO,
)
from data_rentgen.dto.operation import OperationDTO
from data_rentgen.utils.uuid import generate_incremental_uuid, generate_static_uuid


@dataclass
class ColumnLineageDTO:
    created_at: datetime
    operation: OperationDTO
    source_dataset: DatasetDTO
    target_dataset: DatasetDTO
    dataset_column_relations: InitVar[list[DatasetColumnRelationDTO]] = []  # noqa: RUF008
    _dataset_column_relations: dict[tuple, DatasetColumnRelationDTO] = field(default_factory=dict, init=False)

    def __post_init__(self, dataset_column_relations: list[DatasetColumnRelationDTO]):
        self._dataset_column_relations = {item.unique_key: item for item in dataset_column_relations}

    @property
    def column_relations(self) -> list[DatasetColumnRelationDTO]:
        return list(self._dataset_column_relations.values())

    def add_dataset_column_relation(self, relation: DatasetColumnRelationDTO):
        key = relation.unique_key
        existing_relation = self._dataset_column_relations.get(key)
        if existing_relation:
            existing_relation.merge(relation)
        else:
            self._dataset_column_relations[key] = relation

    def has_relations(self) -> bool:
        return bool(self._dataset_column_relations)

    @property
    def unique_key(self):
        return (self.operation.unique_key, self.source_dataset.unique_key, self.target_dataset.unique_key)

    def generate_id(self) -> UUID:
        # Instead of using UniqueConstraint on multiple fields, one of which (schema_id) can be NULL,
        # use them to calculate unique id.
        # This property could be accessed only after all nested ids are fetched from DB
        id_components = [
            str(self.operation.id),
            str(self.source_dataset.id),
            str(self.target_dataset.id),
            str(self.fingerprint),
        ]
        return generate_incremental_uuid(self.created_at, ".".join(id_components))

    @cached_property
    def fingerprint(self) -> UUID:
        id_components = sorted((*item.unique_key, item.type) for item in self.column_relations)
        str_components = [".".join(map(str, item)) for item in id_components]
        return generate_static_uuid(",".join(str_components))

    def merge(self, new: ColumnLineageDTO) -> ColumnLineageDTO:
        self.created_at = min([new.created_at, self.created_at])
        self.operation.merge(new.operation)
        self.source_dataset.merge(new.source_dataset)
        self.target_dataset.merge(new.target_dataset)
        for column_relation in new._dataset_column_relations.values():
            self.add_dataset_column_relation(column_relation)
        return self
