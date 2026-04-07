# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.location import LocationResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.server.schemas.v1.tag import TagResponseV1


class DatasetSchemaFieldV1(BaseModel):
    name: str
    type: str | None = Field(default=None)
    description: str | None = Field(default=None)
    fields: list[DatasetSchemaFieldV1] = Field(description="Nested fields", default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class DatasetSchemaV1(BaseModel):
    id: str = Field(description="Schema id", coerce_numbers_to_str=True)
    fields: list[DatasetSchemaFieldV1] = Field(description="Schema fields")
    relevance_type: Literal["EXACT_MATCH", "LATEST_KNOWN"] | None = Field(
        description="Relevance of schema",
        default="LATEST_KNOWN",
    )
    model_config = ConfigDict(from_attributes=True)


class DatasetResponseV1(BaseModel):
    id: str = Field(description="Dataset id", coerce_numbers_to_str=True)
    location: LocationResponseV1 = Field(description="Corresponding Location")
    name: str = Field(description="Dataset name")
    external_id: str | None = Field(description="External ID for integration with other systems")
    external_url: str | None = Field(description="Link to dataset in a external system")
    schema: DatasetSchemaV1 | None = Field(  # type: ignore[assignment]
        description="Schema",
        default=None,
        # pydantic models have reserved "schema" attribute, using alias
        serialization_alias="schema",
    )

    model_config = ConfigDict(from_attributes=True)


class DatasetDetailedResponseV1(BaseModel):
    id: str = Field(description="Dataset id", coerce_numbers_to_str=True)
    data: DatasetResponseV1 = Field(description="Dataset data")
    tags: list[TagResponseV1] = Field(default_factory=list, description="Dataset tags")

    model_config = ConfigDict(from_attributes=True)


class DatasetPaginateQueryV1(PaginateQueryV1):
    """Query params for Dataset paginate request."""

    dataset_id: list[int] = Field(
        default_factory=list,
        description="Ids of datasets to fetch specific items only",
    )
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query, partial matching by dataset name/location",
        examples=["my dataset"],
    )
    tag_value_id: list[int] = Field(
        default_factory=list,
        description=(
            "Get datasets having specific tag values assigned. "
            "If multiple values are passed, dataset should have all of them (AND, not OR)"
        ),
        examples=[[123]],
    )
    location_id: list[int] = Field(
        default_factory=list,
        description="Ids of locations the dataset belongs to",
        examples=[[123]],
    )
    location_type: list[str] = Field(
        default_factory=list,
        description="Types of locations the dataset belongs to",
        examples=[["yarn"]],
    )

    model_config = ConfigDict(extra="forbid")
