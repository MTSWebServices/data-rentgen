# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod

from data_rentgen.dto import (
    JobDependencyDTO,
    JobDTO,
    RunDTO,
    RunStatusDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageJobIdentifier,
    OpenLineageParentJob,
    OpenLineageParentRunFacet,
    OpenLineageRunTagsFacetField,
)


class RunExtractorMixin(ABC):
    @abstractmethod
    def extract_job(self, job: OpenLineageJob) -> JobDTO:
        pass

    @abstractmethod
    def extract_pure_job(self, job: OpenLineageJob | OpenLineageParentJob | OpenLineageJobIdentifier) -> JobDTO:
        pass

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        """
        Extract RunDTO from specific event
        """
        run = self.extract_pure_run(event)
        if run.parent_run:
            run.job.parent_job = run.parent_run.job
        self._enrich_run_status(run, event)
        self._add_engine_version_tag(run, event)
        self._add_openlineage_adapter_version_tag(run, event)
        self._add_openlineage_client_version_tag(run, event)
        self._enrich_run_tags(run, event)
        self._enrich_nominal_times(run, event)
        self._enrich_job_dependencies(run, event)
        return run

    def extract_pure_run(self, event: OpenLineageRunEvent) -> RunDTO:
        return RunDTO(
            id=event.run.runId,  # type: ignore [arg-type]
            job=self.extract_job(event.job),
            parent_run=self.extract_parent_run(event.run.facets.parent) if event.run.facets.parent else None,
        )

    def extract_parent_run(self, facet: OpenLineageParentRunFacet | OpenLineageRunEvent) -> RunDTO:
        """
        Extract RunDTO from parent run reference
        """
        return RunDTO(
            id=facet.run.runId,
            job=self.extract_pure_job(facet.job),
        )

    def _enrich_run_status(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        match event.eventType:
            case OpenLineageRunEventType.START:
                run.started_at = event.eventTime
                run.status = RunStatusDTO.STARTED
            case OpenLineageRunEventType.RUNNING:
                run.status = RunStatusDTO.STARTED
            case OpenLineageRunEventType.COMPLETE:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.SUCCEEDED
            case OpenLineageRunEventType.FAIL:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.FAILED
            case OpenLineageRunEventType.ABORT:
                run.ended_at = event.eventTime
                run.status = RunStatusDTO.KILLED
            case OpenLineageRunEventType.OTHER:
                # OTHER is used only to update run statistics
                pass
        return run

    def _add_engine_version_tag(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if not event.run.facets.processing_engine:
            return run

        engine_tag_value = TagValueDTO(
            tag=TagDTO(name=f"{event.run.facets.processing_engine.name.lower()}.version"),
            value=str(event.run.facets.processing_engine.version),
        )
        run.job.tag_values.add(engine_tag_value)
        return run

    def _add_openlineage_adapter_version_tag(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if event.run.facets.processing_engine and event.run.facets.processing_engine.openlineageAdapterVersion:
            adapter_tag_value = TagValueDTO(
                tag=TagDTO(name="openlineage_adapter.version"),
                value=str(event.run.facets.processing_engine.openlineageAdapterVersion),
            )
            run.job.tag_values.add(adapter_tag_value)
        return run

    def _add_openlineage_client_version_tag(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if not event.run.facets.tags:
            return run

        run_tags: list[OpenLineageRunTagsFacetField] = []
        for raw_tag in event.run.facets.tags.tags:
            if raw_tag.key != "openlineage_client_version":
                run_tags.append(raw_tag)
                continue

            # https://github.com/OpenLineage/OpenLineage/blob/1.42.1/client/python/src/openlineage/client/client.py#L460
            client_tag_value = TagValueDTO(
                tag=TagDTO(name="openlineage_client.version"),
                value=raw_tag.value,
            )
            run.job.tag_values.add(client_tag_value)

        # avoid passing this tag to _enrich_run_tags
        event.run.facets.tags.tags[:] = run_tags
        return run

    # Job and Run tags are different from OpenLineage spec perspective,
    # but are messed up in integrations, so all tags are merged into job
    def _enrich_run_tags(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if not event.run.facets.tags:
            return run

        for raw_tag in event.run.facets.tags.tags:
            tag_value = TagValueDTO(
                tag=TagDTO(name=raw_tag.key),
                value=raw_tag.value,
            )
            run.job.tag_values.add(tag_value)
        return run

    def _enrich_nominal_times(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if not event.run.facets.nominalTime:
            return run

        run.expected_start_at = event.run.facets.nominalTime.nominalStartTime
        run.expected_end_at = event.run.facets.nominalTime.nominalEndTime
        if run.expected_start_at == run.expected_end_at:
            run.expected_end_at = None

        return run

    def _enrich_job_dependencies(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if not event.run.facets.jobDependencies:
            return run

        for upstream in event.run.facets.jobDependencies.upstream:
            run.job_dependencies.append(
                JobDependencyDTO(
                    from_job=self.extract_pure_job(upstream.job),
                    to_job=run.job,
                    type=upstream.dependency_type,
                ),
            )

        for downstream in event.run.facets.jobDependencies.downstream:
            run.job_dependencies.append(
                JobDependencyDTO(
                    from_job=run.job,
                    to_job=self.extract_pure_job(downstream.job),
                    type=downstream.dependency_type,
                ),
            )

        return run
