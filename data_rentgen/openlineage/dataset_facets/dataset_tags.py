# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.dataset_facets.base import OpenLineageDatasetFacet


class OpenLineageDatasetTagsFacetField(OpenLineageBase):
    """Dataset tags field type.
    See [TagsDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsDatasetFacet.json).
    """

    key: str = Field(description="Key that identifies the tag")
    value: str = Field(description="The value of the field")
    source: str | None = Field(default=None, description="The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.")
    field: str | None = Field(default=None, description="Identifies the field in a dataset if a tag applies to one")


class OpenLineageDatasetTagsFacet(OpenLineageDatasetFacet):
    """Dataset facet describing dataset tags.
    See [TagsDatasetFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DatasetTypeDatasetFacet.json).
    """

    tags: list[OpenLineageDatasetTagsFacetField] = Field(
        default_factory=list,
        description="The tags applied to the dataset facet",
    )
