# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkGroupDTO,
    DatasetSymlinkTypeDTO,
    InputDTO,
    OperationDTO,
    OutputDTO,
    OutputTypeDTO,
    SchemaDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageDataset,
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.dataset_facets import OpenLineageSchemaField
from data_rentgen.openlineage.run_event import OpenLineageRunEvent

SQL_QUERY_SYNTAX = re.compile(
    r"\b(?P<query_type>MERGE|INSERT|UPDATE|DELETE|CREATE|RENAME|TRUNCATE|DROP(?!\sCOLUMN)|COPY)\s",
    re.IGNORECASE | re.DOTALL,
)
# Alter has lowest priority
ALTER_SYNTAX = re.compile(r"\bALTER\s", re.IGNORECASE | re.DOTALL)
QUERY_TYPE_TO_OUTPUT_TYPE = {
    "MERGE": OutputTypeDTO.MERGE,
    "INSERT": OutputTypeDTO.APPEND,
    "UPDATE": OutputTypeDTO.UPDATE,
    "DELETE": OutputTypeDTO.DELETE,
    "CREATE": OutputTypeDTO.CREATE,
    "RENAME": OutputTypeDTO.RENAME,
    "ALTER": OutputTypeDTO.ALTER,
    "TRUNCATE": OutputTypeDTO.TRUNCATE,
    "DROP": OutputTypeDTO.DROP,
    "COPY": OutputTypeDTO.APPEND,
}

METASTORE = DatasetSymlinkTypeDTO.METASTORE
WAREHOUSE = DatasetSymlinkTypeDTO.WAREHOUSE


class IOExtractorMixin(ABC):
    io_time_resolution = timedelta(hours=1)

    @abstractmethod
    def extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
    ) -> tuple[DatasetDTO, list[DatasetSymlinkGroupDTO]]:
        pass

    def extract_io_created_at(self, operation: OperationDTO, event: OpenLineageRunEvent) -> datetime:
        """
        Get `created_at` for input/output/column lineage rows.

        For short living operations we create one input/output/column lineage row per operation+dataset,
        and just update the same row every time we receive an event with same operation,
        to store new number of rows/bytes/files.

        For long running operations (mostly streaming), this behavior lead to the following situation:

        * Flink job started 01.05.2025, sending a `START` event.
        * This lead to creating input/output with `created_at=2025-05-01`
        * For every checkpoint interval (60 seconds) OpenLineage sends `RUNNING` event,
          which only updates IO statistics columns
        * User fetches lineage graph for this job with `since=2025-06-20` (week ago).
        * Because `input.created_at`/`output.created_at` don't match `since`,
          there is no lineage for this Flink job for this interval.
          But Flink job is working, it is not dead, it performs some IO and sends us OpenLineage events.

        Creating a new input/output/column lineage row every received event
        (`START`, `RUNNING`, `RUNNING`, ..., `RUNNING`, `COMPLETE`) will flood the database with useless data,
        as most of these rows will be skipped during lineage graph resolution.

        Creating new rows only for STREAMING operations will not solve the problem,
        because there are long running BATCH jobs as well which produce RUNNING events.

        So we create new input/output/column lineage for *every hour* since operation was started.
        Rows look like that:
        |==================|==============|============|==========|===========|===========|
        | created_at       | operation_id | dataset_id | num_rows | num_files | num_bytes |
        |------------------|--------------|------------|----------|-----------|-----------|
        | 2025-06-20 00:00 | operation1   | dataset1   | 0        | 0         | 0         |
        | 2025-06-20 01:00 | operation1   | dataset1   | 15       | 0         | 1_000     |
        | 2025-06-20 02:00 | operation1   | dataset1   | 30       | 0         | 2_000     |
        | 2025-06-20 03:00 | operation1   | dataset1   | 45       | 0         | 3_000     |
        |==================|==============|============|==========|===========|===========|

        Time resolution is hardcoded, but can be made configurable in future.
        """
        since_start = event.eventTime.timestamp() - operation.created_at.timestamp()

        if since_start < 0:
            # avoid moving back in time
            return operation.created_at

        whole_ticks_since_start = timedelta(seconds=since_start) // self.io_time_resolution
        return operation.created_at + whole_ticks_since_start * self.io_time_resolution

    def extract_input(
        self,
        operation: OperationDTO,
        dataset: OpenLineageInputDataset,
        event: OpenLineageRunEvent,
    ) -> tuple[InputDTO, list[DatasetSymlinkGroupDTO]]:
        """
        Extract InputDTO with optional symlinks
        """
        resolved_dataset_dto, symlinks = self.extract_dataset_and_symlinks(dataset)
        created_at = self.extract_io_created_at(operation, event)

        result = InputDTO(
            created_at=created_at,
            operation=operation,
            dataset=resolved_dataset_dto,
            schema=self.extract_schema(dataset),
        )
        if dataset.inputFacets.inputStatistics:
            result.num_rows = dataset.inputFacets.inputStatistics.rows
            result.num_bytes = dataset.inputFacets.inputStatistics.bytes
            result.num_files = dataset.inputFacets.inputStatistics.files
        return result, symlinks

    def extract_output(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
        event: OpenLineageRunEvent,
    ) -> tuple[OutputDTO, list[DatasetSymlinkGroupDTO]]:
        """
        Extract OutputDTO with optional symlinks
        """
        resolved_dataset_dto, symlinks = self.extract_dataset_and_symlinks(dataset)
        created_at = self.extract_io_created_at(operation, event)

        result = OutputDTO(
            created_at=created_at,
            operation=operation,
            dataset=resolved_dataset_dto,
            type=self._extract_output_type(operation, dataset) or OutputTypeDTO.UNKNOWN,
            schema=self.extract_schema(dataset),
        )
        if dataset.outputFacets.outputStatistics:
            result.num_rows = dataset.outputFacets.outputStatistics.rows
            result.num_bytes = dataset.outputFacets.outputStatistics.bytes
            result.num_files = dataset.outputFacets.outputStatistics.files

        return result, symlinks

    def _extract_output_type(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> OutputTypeDTO | None:
        if dataset.facets.lifecycleStateChange:
            return OutputTypeDTO[dataset.facets.lifecycleStateChange.lifecycleStateChange]
        if operation.sql_query:
            return self._extract_output_type_from_sql(operation.sql_query.query)
        return None

    def _extract_output_type_from_sql(self, sql: str) -> OutputTypeDTO | None:
        found = SQL_QUERY_SYNTAX.search(sql)
        if found:
            return QUERY_TYPE_TO_OUTPUT_TYPE[found.group("query_type").upper()]
        found = ALTER_SYNTAX.search(sql)
        if found:
            return OutputTypeDTO.ALTER
        return None

    def _schema_field_to_json(self, field: OpenLineageSchemaField):
        result: dict = {
            "name": field.name,
        }
        if field.type:
            result["type"] = field.type

        description_ = (field.description or "").strip()
        if description_:
            result["description"] = description_

        if field.fields:
            result["fields"] = [self._schema_field_to_json(f) for f in field.fields]
        return result

    def extract_schema(self, dataset: OpenLineageDataset) -> SchemaDTO | None:
        """
        Extract SchemaDTO from specific dataset
        """
        if not dataset.facets.datasetSchema:
            return None

        fields = dataset.facets.datasetSchema.fields
        if not fields:
            return None

        return SchemaDTO(fields=[self._schema_field_to_json(field) for field in fields])
