# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field


class ApplicationVersionSettings(BaseModel):
    """X-Application-Version Middleware Settings.

    Examples
    --------

    ```yaml title="config.yml"
    server:
      application_version:
        enabled: true
        header_name: X-Application-Version
    ```
    """

    enabled: bool = Field(default=True, description="Set to `True` to enable middleware")
    header_name: str = Field(
        default="X-Application-Version",
        description="Name of response header which is filled up with application version number",
    )
