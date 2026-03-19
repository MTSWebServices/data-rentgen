# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from pydantic import Field

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageNominalTimeRunFacet(OpenLineageRunFacet):
    """Run facet describing nominal start and end time.
    See [NominalTimeRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/NominalTimeRunFacet.json).
    """

    nominalStartTime: datetime | None = Field(
        default=None,
        examples=["2020-12-17T03:00:00.000Z"],
        description=(
            "An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)"
            "timestamp representing the nominal start time (included) of the run."
            "Represents the scheduled/expected time, not the actual start time."
        ),
    )
    nominalEndTime: datetime | None = Field(
        default=None,
        examples=["2020-12-17T03:05:00.000Z"],
        description=(
            "An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)"
            "timestamp representing the nominal end time (excluded) of the run."
            "Represents the scheduled/expected time, not the actual end time."
        ),
    )
