# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import urlparse

from data_rentgen.dto import (
    JobDTO,
    JobTypeDTO,
    LocationDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.run_facets import (
    OpenLineageJobIdentifier,
    OpenLineageParentJob,
)


class JobExtractorMixin:
    def extract_job(self, job: OpenLineageJob) -> JobDTO:
        """
        Extract JobDTO from specific job
        """
        job_dto = JobDTO(
            name=job.name,
            location=self._extract_job_location(job),
            type=self._extract_job_type(job),
        )
        return self._enrich_job_tags(job_dto, job)

    def extract_pure_job(
        self,
        job: OpenLineageJob | OpenLineageParentJob | OpenLineageJobIdentifier,
    ) -> JobDTO:
        """
        Extract JobDTO from parent job reference
        """
        return JobDTO(
            name=job.name,
            location=self._extract_job_location(job),
        )

    def _extract_job_location(
        self,
        job: OpenLineageJob | OpenLineageParentJob | OpenLineageJobIdentifier,
    ) -> LocationDTO:
        # hostname and scheme are normalized to lowercase for uniqueness
        url = urlparse(job.namespace.lower())
        scheme = url.scheme or "unknown"
        netloc = url.netloc or url.path
        return LocationDTO(
            type=scheme,
            name=netloc,
            addresses={f"{scheme}://{netloc}"},
        )

    def _extract_job_type(self, job: OpenLineageJob) -> JobTypeDTO | None:
        if job.facets.jobType:
            integration_type = job.facets.jobType.integration
            job_type = job.facets.jobType.jobType
            type_ = f"{integration_type}_{job_type}" if job_type else integration_type
            # job_type are always upper case
            return JobTypeDTO(type=type_.upper())

        return None

    def _enrich_job_tags(self, job_dto: JobDTO, job: OpenLineageJob) -> JobDTO:
        if not job.facets.tags:
            return job_dto

        for raw_tag in job.facets.tags.tags:
            tag_value = TagValueDTO(
                tag=TagDTO(name=raw_tag.key),
                value=raw_tag.value,
            )
            job_dto.tag_values.add(tag_value)
        return job_dto
