# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import re

from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.dto import DatasetDTO, DatasetSymlinkDTO, OperationDTO, RunDTO, UserDTO
from data_rentgen.openlineage.dataset import OpenLineageDataset
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinkType,
)
from data_rentgen.openlineage.run_event import OpenLineageRunEvent

PARTITION_PATH_PATTERN: re.Pattern = re.compile("^(.*?)/[^/=]+=")

logger = logging.getLogger(__name__)


def first_non_null(value: str | None) -> str | None:
    return value.split(",")[0] if value else None


def sql_to_operation_name(value: str):
    return " ".join(line.strip() for line in value.splitlines()).strip()


class SparkExtractor(GenericExtractor):
    def match(self, event: OpenLineageRunEvent) -> bool:
        return bool(event.job.facets.jobType and event.job.facets.jobType.integration == "SPARK")

    def is_operation(self, event: OpenLineageRunEvent) -> bool:
        return event.job.facets.jobType.jobType != "APPLICATION"  # type: ignore[union-attr]

    def extract_run(self, event: OpenLineageRunEvent) -> RunDTO:
        run = super().extract_run(event)
        self._enrich_run_identifiers(run, event)
        return run

    def _enrich_run_identifiers(self, run: RunDTO, event: OpenLineageRunEvent):
        app = event.run.facets.spark_applicationDetails
        if not app:
            return run

        # https://github.com/OpenLineage/OpenLineage/pull/2719
        run.external_id = app.applicationId
        run.user = UserDTO(name=app.userName)

        run.persistent_log_url = first_non_null(app.historyUrl)
        if app.proxyUrl:
            run.running_log_url = first_non_null(app.proxyUrl)
        else:
            run.running_log_url = first_non_null(app.uiWebUrl)
        return run

    def extract_operation(self, event: OpenLineageRunEvent) -> OperationDTO:
        # For Spark, SQL_JOB --parent-> SPARK_APPLICATION = operation -> run,
        # and parent is always here.
        run = self.extract_parent_run(event.run.facets.parent)  # type: ignore[arg-type]
        # Workaround for https://github.com/OpenLineage/OpenLineage/issues/3846
        self._enrich_run_identifiers(run, event)
        self._enrich_run_tags(run, event)
        operation = super()._extract_operation(event, run)

        # in some cases, operation name may contain raw SELECT query with newlines. use spaces instead.
        # Spark execution name is "applicationName.operationName", drop prefix.
        operation.name = sql_to_operation_name(event.job.name).removeprefix(run.job.name + ".")

        spark_job_details = event.run.facets.spark_jobDetails
        if spark_job_details:
            operation.position = spark_job_details.jobId
            operation.group = spark_job_details.jobGroup
            operation.description = spark_job_details.jobDescription
        return operation

    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        dataset_dto = super()._extract_dataset_ref(dataset)

        # convert /some/long/path/with=partition/another=abc to /some/long/path
        if "=" in dataset_dto.name and "/" in dataset_dto.name:
            name_with_partitions = PARTITION_PATH_PATTERN.match(dataset_dto.name)
            if name_with_partitions:
                dataset_dto.name = name_with_partitions.group(1)
        return dataset_dto

    def _extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
        symlink_identifiers: list[OpenLineageSymlinkIdentifier],
    ) -> tuple[DatasetDTO, list[DatasetSymlinkDTO]]:
        table_symlinks = [
            identifier for identifier in symlink_identifiers if identifier.type == OpenLineageSymlinkType.TABLE
        ]
        if not table_symlinks:
            return super()._extract_dataset_and_symlinks(dataset, symlink_identifiers)

        # We are swapping the dataset with its TABLE symlink to create a cleaner lineage.
        # For example, by replacing an HDFS path with its corresponding Hive table, which is preferred by users.
        # Discussion on this issue: https://github.com/OpenLineage/OpenLineage/issues/2718

        # TODO: add support for multiple TABLE symlinks
        if len(table_symlinks) > 1:
            logger.warning(
                "Dataset has more than one TABLE symlink. "
                "Only the first one will be used for replacement. Symlink name: %s",
                table_symlinks[0].name,
            )

        location_dataset_dto = self.extract_dataset(dataset)
        table_dataset_dto = self._extract_dataset_ref(table_symlinks[0])

        # Swap datasets in column lineage as well
        dataset_cache_key = (dataset.namespace, dataset.name)
        self._dataset_ref_to_dto_cache[dataset_cache_key] = table_dataset_dto

        return (
            table_dataset_dto,
            self._connect_dataset_with_symlinks(
                location_dataset_dto,
                table_dataset_dto,
                OpenLineageSymlinkType.TABLE,
            ),
        )
