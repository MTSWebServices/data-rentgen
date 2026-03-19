# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageRunTagsFacetField(OpenLineageBase):
    """Run tags field type.
    See [TagsRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsRunFacet.json).
    """

    key: str = Field(description="Key that identifies the tag")
    value: str = Field(description="The value of the field")
    source: str | None = Field(default=None, description="The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.")


class OpenLineageRunTagsFacet(OpenLineageRunFacet):
    """Run facet describing run tags.
    See [TagsRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/RunTypeRunFacet.json).
    """

    tags: list[OpenLineageRunTagsFacetField] = Field(
        default_factory=list,
        description="The tags applied to the run facet",
    )
