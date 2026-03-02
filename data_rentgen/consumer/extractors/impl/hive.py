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
from data_rentgen.openlineage.dataset import OpenLineageOutputDataset
from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.openlineage.run_facets import (
    OpenLineageHiveQueryInfoRunFacet,
    OpenLineageHiveSessionInfoRunFacet,
)
from data_rentgen.utils.uuid import generate_incremental_uuid


class HiveExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        if event.job.namespace.startswith("hive://"):
            return True
        # Added in OpenLineage 1.35.0 https://github.com/OpenLineage/OpenLineage/pull/3789
        return bool(event.job.facets.jobType and event.job.facets.jobType.integration == "HIVE")

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        # All events produced by Hive integration are queries == operations
        return True

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        # Hive produce only hive query events, but no events for hive session:
        # https://github.com/OpenLineage/OpenLineage/issues/3784
        # But we treat queries as operations, and operations should be bound to run (session) for grouping.
        # So we create run artificially using hive_session facet

        # sessionId is UUIDv4, but we have creationTime, so it's possible to generate UUIDv7
        hive_session = cast("OpenLineageHiveSessionInfoRunFacet", event.run.facets.hive_session)
        run_id = generate_incremental_uuid(hive_session.creationTime, hive_session.sessionId)

        # By default, Hive job name is in format "createtable_as_select.targetdb.target_table".
        # If user set custom job name, use it as a session name. Otherwise generate session name as "username.ip"
        hive_query = cast("OpenLineageHiveQueryInfoRunFacet", event.run.facets.hive_query)
        job_name = event.job.name
        if job_name.startswith(hive_query.operationName.lower() + "."):
            job_name = f"{hive_session.username}@{hive_session.clientIp}"

        user: UserDTO | None = None
        if hive_session.username not in ("anonymous", "hive"):
            user = UserDTO(name=hive_session.username)

        run = RunDTO(
            id=run_id,
            job=JobDTO(
                name=job_name,
                location=self._extract_job_location(event.job),
                type=JobTypeDTO(type="HIVE_SESSION"),
            ),
            parent_run=self.extract_parent_run(event.run.facets.parent) if event.run.facets.parent else None,
            started_at=hive_session.creationTime,
            status=RunStatusDTO.STARTED,  # Hive doesn't send events when session stops
            external_id=hive_session.sessionId,
            user=user,
        )
        if run.parent_run:
            run.job.parent_job = run.parent_run.job
        self._add_engine_version_tag(run, event)
        self._add_openlineage_adapter_version_tag(run, event)
        self._add_openlineage_client_version_tag(run, event)
        self._enrich_run_tags(run, event)
        return run

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        run = self.extract_run(event)

        hive_query = cast("OpenLineageHiveQueryInfoRunFacet", event.run.facets.hive_query)
        operation = OperationDTO(
            id=event.run.runId,
            run=run,
            name=hive_query.queryId,
            description=hive_query.operationName,
            type=self._extract_operation_type(event),
            sql_query=self._extract_sql_query(event),
        )
        self._enrich_operation_status(operation, event)
        return operation

    def _extract_output_type(
        self,
        operation: OperationDTO,
        dataset: OpenLineageOutputDataset,
    ) -> OutputTypeDTO:
        match operation.description:
            case None:
                return OutputTypeDTO(0)
            case value if value.startswith("CREATE"):
                return OutputTypeDTO.CREATE
            case value if value.startswith("ALTER"):
                return OutputTypeDTO.ALTER
            case value if value.startswith("DROP"):
                return OutputTypeDTO.DROP
            case value if value.startswith("TRUNCATE"):
                return OutputTypeDTO.TRUNCATE
        return OutputTypeDTO.APPEND
