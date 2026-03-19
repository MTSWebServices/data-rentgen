# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import (
    OpenLineageDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.column_lineage import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
)
from data_rentgen.openlineage.dataset_facets.dataset_tags import (
    OpenLineageDatasetTagsFacet,
    OpenLineageDatasetTagsFacetField,
)
from data_rentgen.openlineage.dataset_facets.documentation import (
    OpenLineageDocumentationDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.input_statistics import (
    OpenLineageInputStatisticsInputDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.lifecycle_change import (
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageDatasetPreviousIdentifier,
    OpenLineageLifecycleStateChangeDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.output_statistics import (
    OpenLineageOutputStatisticsOutputDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.schema import (
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.openlineage.dataset_facets.symlinks import (
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)

__all__ = [
    "OpenLineageColumnLineageDatasetFacet",
    "OpenLineageColumnLineageDatasetFacetField",
    "OpenLineageColumnLineageDatasetFacetFieldRef",
    "OpenLineageColumnLineageDatasetFacetFieldTransformation",
    "OpenLineageDatasetFacet",
    "OpenLineageDatasetFacets",
    "OpenLineageDatasetLifecycleStateChange",
    "OpenLineageDatasetPreviousIdentifier",
    "OpenLineageDatasetTagsFacet",
    "OpenLineageDatasetTagsFacetField",
    "OpenLineageDocumentationDatasetFacet",
    "OpenLineageInputDatasetFacets",
    "OpenLineageInputStatisticsInputDatasetFacet",
    "OpenLineageLifecycleStateChangeDatasetFacet",
    "OpenLineageOutputDatasetFacets",
    "OpenLineageOutputStatisticsOutputDatasetFacet",
    "OpenLineageSchemaDatasetFacet",
    "OpenLineageSchemaField",
    "OpenLineageSymlinkIdentifier",
    "OpenLineageSymlinkType",
    "OpenLineageSymlinksDatasetFacet",
]


class OpenLineageDatasetFacets(OpenLineageBase):
    """All possible dataset facets.
    See [Dataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    documentation: OpenLineageDocumentationDatasetFacet | None = None
    lifecycleStateChange: OpenLineageLifecycleStateChangeDatasetFacet | None = None
    datasetSchema: OpenLineageSchemaDatasetFacet | None = Field(default=None, alias="schema")
    symlinks: OpenLineageSymlinksDatasetFacet | None = None
    columnLineage: OpenLineageColumnLineageDatasetFacet | None = None
    tags: OpenLineageDatasetTagsFacet | None = None


class OpenLineageInputDatasetFacets(OpenLineageBase):
    """All possible input dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    inputStatistics: OpenLineageInputStatisticsInputDatasetFacet | None = None


class OpenLineageOutputDatasetFacets(OpenLineageBase):
    """All possible output dataset facets.
    See [InputDataset](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    outputStatistics: OpenLineageOutputStatisticsOutputDatasetFacet | None = None
