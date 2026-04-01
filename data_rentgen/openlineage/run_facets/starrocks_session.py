# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import UUID7, Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageStarRocksSessionInfoRunFacet(OpenLineageRunFacet):
    """Run facet describing StarRocks session."""

    user: str = Field(examples=["myuser"])
    sessionId: UUID7 = Field(examples=["019d455f-cf2a-7b1e-92d4-8f5c3e9a7b2d"])
    clientIp: str = Field(examples=["11.22.33.44"])
