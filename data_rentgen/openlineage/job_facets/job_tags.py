# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageJobTagsFacetField(OpenLineageBase):
    """Job tags field type.
    See [TagsJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/TagsJobFacet.json).
    """

    key: str = Field(description="Key that identifies the tag")
    value: str = Field(description="The value of the field")
    source: str | None = Field(default=None, description="The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.")


class OpenLineageJobTagsFacet(OpenLineageJobFacet):
    """Job facet describing job tags.
    See [TagsJobFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobTypeJobFacet.json).
    """

    tags: list[OpenLineageJobTagsFacetField] = Field(
        default_factory=list,
        description="The tags applied to the job facet",
    )
