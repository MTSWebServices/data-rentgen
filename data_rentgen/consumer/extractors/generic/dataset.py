# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from urllib.parse import urlparse

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkGroupDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinkType,
)

METASTORE = DatasetSymlinkTypeDTO.METASTORE
WAREHOUSE = DatasetSymlinkTypeDTO.WAREHOUSE

# https://github.com/OpenLineage/OpenLineage/issues/4496
# https://github.com/OpenLineage/OpenLineage/issues/4497
# Fixed in OpenLineage 1.47, but not all users upgraded their ETL scripts
SCHEMALESS_DATABASES = {"clickhouse", "mysql"}

# OpenLineage namespaces are not necessarily URLs:
# https://openlineage.io/docs/spec/naming/
# But Data.Rentgen expects them to be actual URLs.
LOCATION_REPLACEMENTS = {
    re.compile(r"^file$"): "file://unknown",
    re.compile(r"^bigquery$"): "bigquery://googleapis.com",
    re.compile(r"^pubsub$"): "pubsub://googleapis.com",
    re.compile(r"^arn:aws:glue:(?P<region>[^:]+):(?P<account>[^:]+)$"): r"awsglue://\g<account>",
    # also there are some pattern which actually mean the same thing
    re.compile(r"^mssql://"): "sqlserver://",
    re.compile(r"^postgresql://"): "postgres://",
}


def _get_symlink_role(type_: OpenLineageSymlinkType) -> DatasetSymlinkTypeDTO:
    return METASTORE if type_ == OpenLineageSymlinkType.TABLE else WAREHOUSE


def _get_opposite_dataset_role(symlink_roles: list[DatasetSymlinkTypeDTO]) -> DatasetSymlinkTypeDTO:
    return WAREHOUSE if METASTORE in symlink_roles else METASTORE


class DatasetExtractorMixin:
    def extract_dataset(self, dataset: OpenLineageDataset) -> DatasetDTO:
        """
        Extract DatasetDTO from input or output OpenLineageDataset
        """
        dataset_dto = self._extract_dataset_ref(dataset)
        return self._enrich_dataset_tags(dataset_dto, dataset)

    def _extract_dataset_ref(
        self,
        dataset: OpenLineageDataset | OpenLineageColumnLineageDatasetFacetFieldRef | OpenLineageSymlinkIdentifier,
    ) -> DatasetDTO:
        location = self._extract_dataset_location(dataset)
        name = dataset.name
        if location.type in SCHEMALESS_DATABASES and name.count(".") == 2:  # noqa: PLR2004
            name = name.split(".", maxsplit=1)[1]
        return DatasetDTO(
            name=name,
            location=location,
        )

    def _extract_dataset_location(
        self,
        dataset: OpenLineageDataset | OpenLineageSymlinkIdentifier | OpenLineageColumnLineageDatasetFacetFieldRef,
    ) -> LocationDTO:
        # hostname and scheme are normalized to lowercase for uniqueness
        namespace = dataset.namespace.lower()
        for pattern, replacement in LOCATION_REPLACEMENTS.items():
            namespace = pattern.sub(replacement, namespace)

        url = urlparse(namespace)
        scheme = url.scheme or "unknown"

        # TODO: handle S3 bucket properly after https://github.com/OpenLineage/OpenLineage/issues/2816
        netloc = url.netloc or url.path
        hosts = list(filter(None, netloc.split(","))) or ["unknown"]
        return LocationDTO(
            type=scheme,
            name=hosts[0],
            addresses={f"{scheme}://{host}" for host in hosts},
        )

    def extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
    ) -> tuple[DatasetDTO, list[DatasetSymlinkGroupDTO]]:
        symlink_identifiers = dataset.facets.symlinks.identifiers if dataset.facets.symlinks else []
        return self._extract_dataset_and_symlinks(dataset, symlink_identifiers)

    def _extract_dataset_and_symlinks(
        self,
        dataset: OpenLineageDataset,
        symlink_identifiers: list[OpenLineageSymlinkIdentifier],
    ) -> tuple[DatasetDTO, list[DatasetSymlinkGroupDTO]]:
        dataset_dto = self.extract_dataset(dataset)
        symlinks = [
            (self._extract_dataset_ref(symlink_identifier), symlink_identifier.type)
            for symlink_identifier in symlink_identifiers
        ]
        return dataset_dto, [self._build_dataset_symlink_group(dataset_dto, symlinks)] if symlinks else []

    def _build_dataset_symlink_group(
        self,
        dataset: DatasetDTO,
        symlinks: list[tuple[DatasetDTO, OpenLineageSymlinkType]],
    ) -> DatasetSymlinkGroupDTO:
        symlink_members = [(symlink, _get_symlink_role(type_)) for symlink, type_ in symlinks]
        dataset_role = _get_opposite_dataset_role([role for _, role in symlink_members])
        members = [(dataset, dataset_role), *symlink_members]
        return DatasetSymlinkGroupDTO(members=members)

    def _enrich_dataset_tags(self, dataset_dto: DatasetDTO, dataset: OpenLineageDataset) -> DatasetDTO:
        if not dataset.facets.tags:
            return dataset_dto

        for raw_tag in dataset.facets.tags.tags:
            tag_value = TagValueDTO(
                tag=TagDTO(name=raw_tag.key),
                value=raw_tag.value,
            )
            dataset_dto.tag_values.add(tag_value)
        return dataset_dto
