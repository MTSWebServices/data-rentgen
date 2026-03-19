# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

from packaging.version import Version as RawVersion
from pydantic import Field, PlainSerializer, PlainValidator, WithJsonSchema
from typing_extensions import Doc

from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet


def validate_version(value) -> RawVersion:
    if not isinstance(value, RawVersion):
        value = value.replace("-SNAPSHOT", "")
        return RawVersion(value)
    return value


Version = Annotated[
    RawVersion,
    PlainValidator(validate_version),
    PlainSerializer(str, return_type=str),
    WithJsonSchema({"type": "string"}),
    Doc("Version"),
]


class OpenLineageProcessingEngineRunFacet(OpenLineageRunFacet):
    """Run facet describing processing engine.
    See [ProcessingEngineRunFacet](https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ProcessingEngineRunFacet.json).
    """

    name: str = Field(examples=["Spark", "Flink", "Hive"])
    version: Version = Field(examples=["3.5.6"])
    openlineageAdapterVersion: Version | None = Field(default=None, examples=["1.36.0"])
