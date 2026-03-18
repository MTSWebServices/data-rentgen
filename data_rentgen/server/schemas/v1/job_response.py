# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.server.schemas.v1.location import LocationResponseV1


class JobResponseV1(BaseModel):
    """Job response"""

    id: str = Field(description="Job id", coerce_numbers_to_str=True)
    parent_job_id: str | None = Field(description="Parent job id", coerce_numbers_to_str=True, default=None)
    location: LocationResponseV1 = Field(description="Corresponding Location")
    name: str = Field(description="Job name")
    type: str = Field(description="Job type")

    model_config = ConfigDict(from_attributes=True)
