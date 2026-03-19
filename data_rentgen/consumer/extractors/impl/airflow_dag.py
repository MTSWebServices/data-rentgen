# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import quote

from packaging.version import Version

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.consumer.extractors.impl.utils import parse_kv_tag
from data_rentgen.dto import JobDTO, RunDTO, RunStartReasonDTO, UserDTO
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import OpenLineageJobTagsFacetField
from data_rentgen.openlineage.run_event import OpenLineageRunEvent
from data_rentgen.openlineage.run_facets import (
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunType,
    OpenLineageRunTagsFacet,
    OpenLineageRunTagsFacetField,
)


class AirflowDagExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(
            event.job.facets.jobType
            and event.job.facets.jobType.integration == "AIRFLOW"
            and event.job.facets.jobType.jobType == "DAG",
        )

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        # Dags never interact with datasets
        return False

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        self._enrich_run_identifiers(run, event)
        self._enrich_run_user(run, event)
        self._enrich_run_start_reason(run, event)
        self._enrich_run_log_url(run, event)
        return run

    def _enrich_run_identifiers(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        dag_run_facet = event.run.facets.airflowDagRun
        if dag_run_facet:
            run.external_id = dag_run_facet.dagRun.run_id
        return run

    def _enrich_run_user(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        dag_run_facet = event.run.facets.airflowDagRun
        if dag_run_facet and dag_run_facet.dag.owner not in (None, "airflow", "***"):
            run.user = UserDTO(name=dag_run_facet.dag.owner)  # type: ignore[arg-type]
        return run

    def _enrich_run_start_reason(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        dag_run_facet = event.run.facets.airflowDagRun
        if not dag_run_facet:
            return run

        if dag_run_facet.dagRun.run_type == OpenLineageAirflowDagRunType.MANUAL:
            run.start_reason = RunStartReasonDTO.MANUAL
        else:
            run.start_reason = RunStartReasonDTO.AUTOMATIC
        return run

    def _enrich_run_log_url(self, run: RunDTO, event: OpenLineageRunEvent):
        dag_run_facet = event.run.facets.airflowDagRun
        if not dag_run_facet:
            return run

        airflow_base_url = event.job.namespace
        if not airflow_base_url.startswith("http"):
            return run

        processing_engine = event.run.facets.processing_engine
        if processing_engine and processing_engine.version >= Version("3.0.0"):
            run.persistent_log_url = self._get_airflow_3_x_plus_dag_run_url(
                airflow_base_url,
                dag_run_facet,
            )
        elif processing_engine and processing_engine.version >= Version("2.3.0"):
            run.persistent_log_url = self._get_airflow_2_3_plus_dag_run_url(
                airflow_base_url,
                dag_run_facet,
            )
        else:
            run.persistent_log_url = self._get_airflow_2_x_dag_run_url(
                airflow_base_url,
                dag_run_facet,
            )

        return run

    def _get_airflow_3_x_plus_dag_run_url(
        self,
        namespace: str,
        dag_run_facet: OpenLineageAirflowDagRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/pull/46942
        # https://github.com/apache/airflow/blob/3.0.1/airflow-core/src/airflow/utils/helpers.py#L199-L207
        dag_id = dag_run_facet.dag.dag_id
        dag_run_id = dag_run_facet.dagRun.run_id
        return f"{namespace}/dags/{dag_id}/runs/{dag_run_id}"

    def _get_airflow_2_3_plus_dag_run_url(
        self,
        namespace: str,
        dag_run_facet: OpenLineageAirflowDagRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/pull/20730
        # https://github.com/apache/airflow/blob/2.9.2/airflow/www/views.py#L2788
        dag_id = dag_run_facet.dag.dag_id
        dag_run_id = quote(dag_run_facet.dagRun.run_id)
        return f"{namespace}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"

    def _get_airflow_2_x_dag_run_url(
        self,
        namespace: str,
        dag_run_facet: OpenLineageAirflowDagRunFacet,
    ) -> str:
        # https://github.com/apache/airflow/blob/2.9.2/airflow/www/views.py#L2975
        dag_id = quote(dag_run_facet.dag.dag_id)
        execution_date = quote(dag_run_facet.dagRun.data_interval_start.isoformat())
        return f"{namespace}/graph?dag_id={dag_id}&execution_date={execution_date}"

    def _enrich_job_tags(self, job_dto: JobDTO, job: OpenLineageJob) -> JobDTO:
        if not job.facets.tags:
            return job_dto

        # Send only by apache-airflow-openlineage-provider 2.4.0+
        # https://github.com/apache/airflow/pull/51303
        job_tags: list[OpenLineageJobTagsFacetField] = []
        for raw_tag in job.facets.tags.tags:
            if raw_tag.key != raw_tag.value:
                # not implemented in OpenLineage yet
                # https://github.com/apache/airflow/issues/61069
                job_tags.append(raw_tag)
                continue

            if ":" not in raw_tag.key:
                # OL integration sends tags like "prod:prod" which are not suitable for DataRentgen
                continue

            key, value, source = parse_kv_tag(
                raw_tag.key,
                default_source=raw_tag.source or "AIRFLOW",
            )
            job_tags.append(OpenLineageJobTagsFacetField(key=key, value=value, source=source))

        job.facets.tags.tags[:] = job_tags
        return super()._enrich_job_tags(job_dto, job)

    def _enrich_run_tags(self, run: RunDTO, event: OpenLineageRunEvent) -> RunDTO:
        if event.run.facets.tags or not event.run.facets.airflowDagRun:
            # user-specified tags in OpenLineage config - bypass
            return super()._enrich_run_tags(run, event)

        # before apache-airflow-openlineage-provider 2.4.0,
        # we can get raw tags list only from dag description

        run_tags: list[OpenLineageRunTagsFacetField] = []
        for tag_str in event.run.facets.airflowDagRun.dag.tags:
            if ":" not in tag_str:
                # see above
                continue

            key, value, source = parse_kv_tag(
                tag_str,
                default_source="AIRFLOW",
            )
            run_tags.append(OpenLineageRunTagsFacetField(key=key, value=value, source=source))

        # facets are immutable
        object.__setattr__(event.run.facets, "tags", OpenLineageRunTagsFacet(tags=run_tags))
        return super()._enrich_run_tags(run, event)
