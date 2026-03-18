# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.job_response import JobResponseV1
from data_rentgen.server.schemas.v1.pagination import PaginateQueryV1
from data_rentgen.server.schemas.v1.run import RunResponseV1
from data_rentgen.server.schemas.v1.tag import TagResponseV1


class JobDetailedResponseV1(BaseModel):
    id: str = Field(description="Job id", coerce_numbers_to_str=True)
    data: JobResponseV1 = Field(description="Job data")
    tags: list[TagResponseV1] = Field(default_factory=list, description="Job tags")
    last_run: RunResponseV1 | None = Field(description="Last run of the job", default=None)

    model_config = ConfigDict(from_attributes=True)


class JobTypesResponseV1(BaseModel):
    """Job types"""

    job_types: list[str] = Field(
        description="List of distinct job types",
        examples=[["SPARK_APPLICATION", "AIRFLOW_DAG"]],
    )

    model_config = ConfigDict(from_attributes=True)


# TODO: This class and several others are duplicate from lineage schemas, maybe we should create common class for both.
class JobEntityV1(BaseModel):
    kind: Literal["JOB"] = Field(default="JOB")
    id: str = Field(description="Id of the Job")

    model_config = ConfigDict(from_attributes=True)


class JobDependencyV1(BaseModel):
    from_: JobEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: JobEntityV1 = Field(description="End point of relation")
    type_: str | None = Field(description="Type of dependency", serialization_alias="type", default=None)


class JobParentEntityRelationV1(BaseModel):
    from_: JobEntityV1 = Field(description="Start point of relation", serialization_alias="from")
    to: JobEntityV1 = Field(description="End point of relation")


class JobDependenciesRelationsV1(BaseModel):
    parents: list[JobParentEntityRelationV1] = Field(description="Parent relations", default_factory=list)
    dependencies: list[JobDependencyV1] = Field(description="Job dependencies", default_factory=list)


class JobDependenciesResponseV1(BaseModel):
    "Job dependencies"

    relations: JobDependenciesRelationsV1 = Field(
        description="Job parents and dependencies relations",
        default_factory=JobDependenciesRelationsV1,
    )
    nodes: dict[str, dict[str, JobResponseV1]] = Field(description="Job nodes", default_factory=dict)


class JobPaginateQueryV1(PaginateQueryV1):
    """Query params for Jobs paginate request."""

    job_id: list[int] = Field(
        default_factory=list,
        description="Ids of jobs to fetch specific items only",
    )
    parent_job_id: list[int] = Field(
        default_factory=list,
        description="Parent Jobs ids",
    )
    search_query: str | None = Field(
        default=None,
        min_length=3,
        description="Search query, partial match by job name or location name/address",
        examples=["my job"],
    )
    job_type: list[str] = Field(
        default_factory=list,
        description="Types of jobs",
        examples=[["SPARK_APPLICATION", "AIRFLOW_DAG"]],
    )
    tag_value_id: list[int] = Field(
        default_factory=list,
        description=(
            "Get jobs having specific tag values assigned. "
            "If multiple values are passed, job should have all of them (AND, not OR)"
        ),
        examples=[[123]],
    )
    location_id: list[int] = Field(
        default_factory=list,
        description="Ids of locations the job started at",
        examples=[[123]],
    )
    location_type: list[str] = Field(
        default_factory=list,
        description="Types of location the job started at",
        examples=[["yarn"]],
    )

    model_config = ConfigDict(extra="forbid")


class JobDependenciesQueryV1(BaseModel):
    start_node_id: int = Field(description="Job id", examples=[42])
    direction: Literal["DOWNSTREAM", "UPSTREAM", "BOTH"] = Field(
        default="BOTH",
        description="Direction of the lineage",
        examples=["DOWNSTREAM", "UPSTREAM", "BOTH"],
    )
    model_config = ConfigDict(extra="ignore")
