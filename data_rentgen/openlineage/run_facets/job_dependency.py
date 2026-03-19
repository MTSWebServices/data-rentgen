# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import UUID7, Field

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


class OpenLineageJobIdentifier(OpenLineageBase):
    """Job identifier.
    See [JobDependenciesRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobDependenciesRunFacet.json).
    """

    namespace: str = Field(examples=["http://my-airflow.domain.com:8081"], json_schema_extra={"format": "uri"})
    name: str = Field(examples=["my_dag.my_task"])


class OpenLineageRunIdentifier(OpenLineageBase):
    """Run identifier.
    See [JobDependenciesRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobDependenciesRunFacet.json).
    """

    runId: UUID7 = Field(examples=["019867d4-1b59-71fe-bc30-3fbd38703700"])


class OpenLineageJobDependency(OpenLineageRunFacet):
    """Job dependency item.
    See [JobDependenciesRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobDependenciesRunFacet.json).
    """

    job: OpenLineageJobIdentifier
    run: OpenLineageRunIdentifier | None = None
    dependency_type: str | None = Field(
        default=None,
        examples=["DIRECT_INVOCATION", "IMPLICIT_DEPENDENCY"],
        description="Implementation specific",
    )
    sequence_trigger_rule: str | None = Field(
        default=None,
        examples=["FINISH_TO_START", "FINISH_TO_FINISH", "START_TO_START"],
        description="Currently ignored",
    )
    status_trigger_rule: str | None = Field(
        default=None,
        examples=["EXECUTE_EVERY_TIME", "EXECUTE_ON_SUCCESS", "EXECUTE_ON_FAILURE"],
        description="Currently ignored",
    )


class OpenLineageJobDependenciesRunFacet(OpenLineageRunFacet):
    """Run facet describing job dependencies.
    See [JobDependenciesRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/JobDependenciesRunFacet.json).
    """

    upstream: list[OpenLineageJobDependency] = Field(default_factory=list)
    downstream: list[OpenLineageJobDependency] = Field(default_factory=list)
    trigger_rule: str | None = Field(
        default=None,
        examples=["ALL_SUCCESS", "ALL_DONE", "ONE_SUCCESS", "NONE_FAILED"],
        description="Currently ignored",
    )
