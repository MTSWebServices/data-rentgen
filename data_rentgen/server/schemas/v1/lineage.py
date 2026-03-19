# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from enum import Enum, IntEnum, IntFlag
from typing import Literal
from uuid import UUID

from pydantic import UUID7, BaseModel, ConfigDict, Field, ValidationInfo, field_serializer, field_validator

from data_rentgen.server.schemas.v1.dataset import DatasetResponseV1
from data_rentgen.server.schemas.v1.job import JobResponseV1
from data_rentgen.server.schemas.v1.operation import OperationResponseV1
from data_rentgen.server.schemas.v1.run import RunResponseV1


class LineageEntityKindV1(str, Enum):
    JOB = "JOB"
    RUN = "RUN"
    OPERATION = "OPERATION"
    DATASET = "DATASET"

    def __str__(self) -> str:
        return self.value


class LineageDirectionV1(str, Enum):
    DOWNSTREAM = "DOWNSTREAM"
    UPSTREAM = "UPSTREAM"
    BOTH = "BOTH"

    def __str__(self) -> str:
        return self.value


class LineageEntityV1(BaseModel):
    kind: LineageEntityKindV1 = Field(description="Type of Lineage entity")
    id: str | UUID = Field(description="Id of Lineage entity")

    model_config = ConfigDict(from_attributes=True)


class BaseLineageQueryV1(BaseModel):
    since: datetime = Field(
        description="",
        examples=["2008-09-15T15:53:00+05:00"],
    )
    until: datetime | None = Field(
        default=None,
        description="",
        examples=["2008-09-15T15:53:00+05:00"],
    )
    direction: LineageDirectionV1 = Field(
        default=LineageDirectionV1.BOTH,
        description="Direction of the lineage",
        examples=["DOWNSTREAM", "UPSTREAM", "BOTH"],
    )
    depth: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Depth of the lineage",
        examples=[1, 3],
    )
    include_column_lineage: bool = Field(default=False, description="Include or not column lineage into response")

    model_config = ConfigDict(extra="forbid")

    @field_validator("until", mode="after")
    @classmethod
    def _check_until(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        since = info.data.get("since")
        if since and value and since.timestamp() >= value.timestamp():
            msg = "'since' should be less than 'until'"
            raise ValueError(msg)
        return value


class DatasetLineageQueryV1(BaseLineageQueryV1):
    start_node_id: int = Field(description="Dataset id", examples=[42])
    granularity: Literal["OPERATION", "RUN", "JOB", "DATASET"] = Field(
        default="RUN",
        description="Granularity of the dataset lineage",
        examples=["OPERATION", "RUN", "JOB"],
    )

    model_config = ConfigDict(extra="forbid")


class JobLineageQueryV1(BaseLineageQueryV1):
    start_node_id: int = Field(description="Job id", examples=[42])
    granularity: Literal["JOB", "RUN"] = Field(
        default="JOB",
        description="Granularity of the job lineage",
        examples=["JOB", "RUN"],
    )


class OperationLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID7 = Field(description="Operation id", examples=["00000000-0000-0000-0000-000000000000"])


class RunLineageQueryV1(BaseLineageQueryV1):
    start_node_id: UUID7 = Field(description="Run id", examples=["00000000-0000-0000-0000-000000000000"])
    granularity: Literal["OPERATION", "RUN"] = Field(
        default="RUN",
        description="Granularity of the run lineage",
        examples=["OPERATION", "RUN"],
    )


class LineageParentRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")


class LineageInputRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)


class OutputTypeV1(IntFlag):
    APPEND = 1

    CREATE = 2
    ALTER = 4
    RENAME = 8

    OVERWRITE = 16

    DROP = 32
    TRUNCATE = 64

    DELETE = 128
    UPDATE = 256
    MERGE = 512

    def __str__(self) -> str:
        return f"{self.name}"


class LineageOutputRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    types: list[OutputTypeV1] = Field(
        description="Type of relation",
        examples=[[OutputTypeV1.APPEND], [OutputTypeV1.TRUNCATE, OutputTypeV1.DROP]],
    )
    last_interaction_at: datetime = Field(description="Last interaction at", examples=["2008-09-15T15:53:00+05:00"])
    num_bytes: int | None = Field(description="Number of bytes", examples=[42], default=None)
    num_rows: int | None = Field(description="Number of rows", examples=[42], default=None)
    num_files: int | None = Field(description="Number of files", examples=[42], default=None)

    @field_serializer("types")
    def serialize_types(self, types: list[OutputTypeV1], _info):
        return [t.name for t in types]


class LineageSymlinkRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: LineageEntityV1 = Field(description="End point of relation")
    type: str = Field(description="Type of relation between datasets", examples=["METASTORE", "WAREHOUSE"])


class ColumnLineageInteractionTypeV1(IntEnum):
    UNKNOWN = 1

    # Direct
    IDENTITY = 2
    TRANSFORMATION = 4
    TRANSFORMATION_MASKING = 8
    AGGREGATION = 16
    AGGREGATION_MASKING = 32

    # Indirect
    FILTER = 64
    JOIN = 128
    GROUP_BY = 256
    SORT = 512
    WINDOW = 1024
    CONDITIONAL = 2048

    def __str__(self) -> str:
        return self.name


class LineageSourceColumnV1(BaseModel):
    field: str = Field(description="Source column", examples=["my_col_1", "my_col_2"])
    types: list[ColumnLineageInteractionTypeV1] = Field(
        description="Type of relation between source and target column",
        examples=[
            [
                ColumnLineageInteractionTypeV1.UNKNOWN,
            ],
            [
                ColumnLineageInteractionTypeV1.TRANSFORMATION,
                ColumnLineageInteractionTypeV1.AGGREGATION,
            ],
            [
                ColumnLineageInteractionTypeV1.JOIN,
            ],
        ],
    )
    last_used_at: datetime = Field(
        description="Last time when transformation was used at",
        examples=["2008-09-15T15:53:00+05:00"],
    )

    @field_serializer("types")
    def serialize_types(self, types: list[ColumnLineageInteractionTypeV1], _info):
        return [type_.name for type_ in types]


class DirectLineageColumnRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Dataset with source columns", serialization_alias="from")
    to: LineageEntityV1 = Field(description="Dataset with target columns")
    fields: dict[str, list[LineageSourceColumnV1]] = Field(
        description="Map of target and source columns with type of direct interaction",
        default_factory=dict,
    )


class IndirectLineageColumnRelationV1(BaseModel):
    from_: LineageEntityV1 = Field(description="Dataset with source columns", serialization_alias="from")
    to: LineageEntityV1 = Field(description="Dataset with target columns")
    fields: list[LineageSourceColumnV1] = Field(
        description="List source columns with type of indirect interaction",
        default_factory=list,
    )


class LineageRelationsResponseV1(BaseModel):
    parents: list[LineageParentRelationV1] = Field(description="Parent relations", default_factory=list)
    symlinks: list[LineageSymlinkRelationV1] = Field(description="Symlink relations", default_factory=list)
    inputs: list[LineageInputRelationV1] = Field(description="Input relations", default_factory=list)
    outputs: list[LineageOutputRelationV1] = Field(description="Input relations", default_factory=list)
    direct_column_lineage: list[DirectLineageColumnRelationV1] = Field(
        description="Direct column lineage relations",
        default_factory=list,
    )
    indirect_column_lineage: list[IndirectLineageColumnRelationV1] = Field(
        description="Indirect column lineage relations",
        default_factory=list,
    )


class LineageNodesResponseV1(BaseModel):
    datasets: dict[str, DatasetResponseV1] = Field(description="Dataset nodes", default_factory=dict)
    jobs: dict[str, JobResponseV1] = Field(description="Job nodes", default_factory=dict)
    runs: dict[UUID, RunResponseV1] = Field(description="Run nodes", default_factory=dict)
    operations: dict[UUID, OperationResponseV1] = Field(description="Operation nodes", default_factory=dict)


class LineageResponseV1(BaseModel):
    relations: LineageRelationsResponseV1 = Field(
        description="Lineage relations",
        default_factory=LineageRelationsResponseV1,
    )
    nodes: LineageNodesResponseV1 = Field(description="Lineage nodes", default_factory=LineageNodesResponseV1)
