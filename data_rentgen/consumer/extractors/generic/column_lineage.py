# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import cast

from data_rentgen.dto import (
    ColumnLineageDTO,
    DatasetColumnRelationDTO,
    DatasetColumnRelationTypeDTO,
    DatasetDTO,
    OperationDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
    OpenLineageSymlinkIdentifier,
)
from data_rentgen.openlineage.run_event import OpenLineageRunEvent

TRANSFORMATION_TYPE_DIRECT = "DIRECT"
TRANSFORMATION_TYPE_INDIRECT = "INDIRECT"

TRANSFORMATION_SUBTYPE_MAP_MASKING = {
    "MASKED": DatasetColumnRelationTypeDTO.TRANSFORMATION_MASKING,
    "TRANSFORMATION": DatasetColumnRelationTypeDTO.TRANSFORMATION_MASKING,
    "AGGREGATION": DatasetColumnRelationTypeDTO.AGGREGATION_MASKING,
}

TRANSFORMATION_SUBTYPE_MAP = {
    "IDENTITY": DatasetColumnRelationTypeDTO.IDENTITY,
    "TRANSFORMATION": DatasetColumnRelationTypeDTO.TRANSFORMATION,
    "AGGREGATION": DatasetColumnRelationTypeDTO.AGGREGATION,
    "FILTER": DatasetColumnRelationTypeDTO.FILTER,
    "JOIN": DatasetColumnRelationTypeDTO.JOIN,
    "GROUP_BY": DatasetColumnRelationTypeDTO.GROUP_BY,
    "SORT": DatasetColumnRelationTypeDTO.SORT,
    "WINDOW": DatasetColumnRelationTypeDTO.WINDOW,
    "CONDITIONAL": DatasetColumnRelationTypeDTO.CONDITIONAL,
}

MISSING = object()


class ColumnLineageExtractorMixin(ABC):
    def __init__(self):
        self._dataset_ref_to_dto_cache: dict[tuple[str, str], DatasetDTO | None] = {}

    @abstractmethod
    def extract_io_created_at(self, operation: OperationDTO, event: OpenLineageRunEvent) -> datetime:
        pass

    @abstractmethod
    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO | None:
        pass

    def _resolve_dataset_ref(
        self,
        dataset_ref: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO | None:
        """
        Column lineage has a lot of dataset references, so this is a hot path which requires caching.
        """
        dataset_cache_key = (dataset_ref.namespace, dataset_ref.name)
        result = self._dataset_ref_to_dto_cache.get(dataset_cache_key, MISSING)
        if result is MISSING:
            result = self._extract_dataset_ref(dataset_ref)
            self._dataset_ref_to_dto_cache[dataset_cache_key] = result
        return cast("DatasetDTO | None", result)

    def extract_column_lineage(
        self,
        operation: OperationDTO,
        output_dataset: OpenLineageOutputDataset,
        event: OpenLineageRunEvent,
    ) -> list[ColumnLineageDTO]:
        """
        Extract ColumnLineageDTO from output dataset, and bound to operation
        """
        if not output_dataset.facets.columnLineage:
            return []

        output_dataset_dto = self._resolve_dataset_ref(output_dataset)
        if not output_dataset_dto:
            return []

        # Grouping column lineage by source+target dataset. This is unique combination within operation,
        # so we can use it to generate the same fingerprint for all dataset column relations
        result: dict[tuple, ColumnLineageDTO] = {}
        created_at = self.extract_io_created_at(operation, event)

        # direct lineage (source_column -> target_column)
        for field, raw_column_lineage in output_dataset.facets.columnLineage.fields.items():
            for input_field in raw_column_lineage.inputFields:
                input_dataset_dto = self._resolve_dataset_ref(input_field)
                if not input_dataset_dto:
                    continue
                column_lineage = ColumnLineageDTO(
                    created_at=created_at,
                    operation=operation,
                    source_dataset=input_dataset_dto,
                    target_dataset=output_dataset_dto,
                )
                column_lineage = result.setdefault(column_lineage.unique_key, column_lineage)

                transformations = input_field.transformations or [self._legacy_transformation(raw_column_lineage)]
                for transformation in transformations:
                    # OL integration for Spark before v1.23
                    # or with columnLineage.datasetLineageEnabled=false (which is still default)
                    # produces INDIRECT lineage for each combination source_column x target_column,
                    # which is almost a cartesian product.
                    # There are a lot of duplicates here, trying to avoid them by merging items immediately.
                    column_relation = DatasetColumnRelationDTO(
                        type=self.extract_dataset_column_relation_type(transformation),
                        source_column=input_field.field,
                        target_column=field if transformation.type == TRANSFORMATION_TYPE_DIRECT else None,
                    )
                    column_lineage.add_dataset_column_relation(column_relation)

        # indirect lineage (source_column -> output_dataset),
        # added to OL since v1.23 and send only when columnLineage.datasetLineageEnabled=true
        for input_field in output_dataset.facets.columnLineage.dataset:
            input_dataset_dto = self._resolve_dataset_ref(input_field)
            if not input_dataset_dto:
                continue
            column_lineage = ColumnLineageDTO(
                created_at=created_at,
                operation=operation,
                source_dataset=input_dataset_dto,
                target_dataset=output_dataset_dto,
            )
            column_lineage = result.setdefault(column_lineage.unique_key, column_lineage)

            transformations = input_field.transformations or [
                OpenLineageColumnLineageDatasetFacetFieldTransformation(type=TRANSFORMATION_TYPE_INDIRECT),
            ]

            for transformation in transformations:
                column_relation = DatasetColumnRelationDTO(
                    type=self.extract_dataset_column_relation_type(transformation),
                    source_column=input_field.field,
                )
                column_lineage.add_dataset_column_relation(column_relation)

        return [column_lineage for column_lineage in result.values() if column_lineage.has_relations()]

    def extract_dataset_column_relation_type(
        self,
        transformation: OpenLineageColumnLineageDatasetFacetFieldTransformation,
    ) -> DatasetColumnRelationTypeDTO:
        result: DatasetColumnRelationTypeDTO | None = None
        if transformation.subtype:
            if transformation.masking:
                result = TRANSFORMATION_SUBTYPE_MAP_MASKING.get(transformation.subtype)
            else:
                result = TRANSFORMATION_SUBTYPE_MAP.get(transformation.subtype)

        return result or DatasetColumnRelationTypeDTO.UNKNOWN

    def _legacy_transformation(self, field: OpenLineageColumnLineageDatasetFacetField):
        type_ = field.transformationType or ""
        return OpenLineageColumnLineageDatasetFacetFieldTransformation(
            type=TRANSFORMATION_TYPE_INDIRECT if type_ == TRANSFORMATION_TYPE_INDIRECT else TRANSFORMATION_TYPE_DIRECT,
            subtype=type_,
            description=field.transformationDescription,
            masking="mask" in type_.lower(),
        )
