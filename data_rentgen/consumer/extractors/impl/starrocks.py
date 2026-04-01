# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import cast

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    OperationDTO,
    OutputTypeDTO,
    RunDTO,
    RunStatusDTO,
    UserDTO,
)
from data_rentgen.dto.sql_query import SQLQueryDTO
from data_rentgen.openlineage.dataset import OpenLineageOutputDataset
from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.openlineage.run_facets import (
    OpenLineageStarRocksSessionInfoRunFacet,
)
from data_rentgen.utils.uuid import extract_timestamp_from_uuid


class StarRocksExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(event.job.facets.jobType and event.job.facets.jobType.integration == "STARROCKS")

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        return bool(event.job.facets.jobType and event.job.facets.jobType.jobType == "QUERY")

    def extract_pure_run(self, event: OpenLineageRunEvent) -> RunDTO:
        # We treat queries as operations, and operations should be bound to run (session) for grouping.
        # So we create run artificially using starrocks_session facet
        starrocks_session = cast("OpenLineageStarRocksSessionInfoRunFacet", event.run.facets.starrocks_session)
        return RunDTO(
            id=starrocks_session.sessionId,
            job=JobDTO(
                name=f"{starrocks_session.user}@{starrocks_session.clientIp}",
                location=self._extract_job_location(event.job),
                type=JobTypeDTO(type="STARROCKS_SESSION"),
            ),
            parent_run=self.extract_parent_run(event.run.facets.parent) if event.run.facets.parent else None,
            started_at=extract_timestamp_from_uuid(starrocks_session.sessionId),
            user=UserDTO(name=starrocks_session.user),
        )

    def _enrich_run_status(self, run: RunDTO, event: OpenLineageRunEvent):
        if self.is_operation(event):
            # for query events we don't know session start time
            run.status = RunStatusDTO.STARTED
            return run

        return super()._enrich_run_status(run, event)

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        run = self.extract_run(event)

        operation = OperationDTO(
            id=event.run.runId,
            run=run,
            name=event.job.name,
            # no started_at == run.started_at
            type=self._extract_operation_type(event),
            sql_query=self._extract_sql_query(event),
        )
        self._enrich_operation_status(operation, event)
        return operation

    def _extract_output_type(  # noqa: PLR0911
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> OutputTypeDTO:
        match operation.sql_query:
            case None:
                return OutputTypeDTO.UNKNOWN
            case SQLQueryDTO(query=query) if query.startswith("INSERT"):
                return OutputTypeDTO.APPEND
            case SQLQueryDTO(query=query) if query.startswith("CREATE"):
                return OutputTypeDTO.CREATE
            case SQLQueryDTO(query=query) if query.startswith("ALTER"):
                return OutputTypeDTO.ALTER
            case SQLQueryDTO(query=query) if query.startswith("DROP"):
                return OutputTypeDTO.DROP
            case SQLQueryDTO(query=query) if query.startswith("TRUNCATE"):
                return OutputTypeDTO.TRUNCATE
        return OutputTypeDTO.UNKNOWN
