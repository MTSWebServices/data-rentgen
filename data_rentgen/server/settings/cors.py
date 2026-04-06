# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CORSSettings(BaseModel):
    """CORS Middleware Settings.

    See `CORSMiddleware <https://www.starlette.io/middleware/#corsmiddleware>`_ documentation.

    .. note::

        You can pass here any extra option supported by ``CORSMiddleware``,
        even if it is not mentioned in documentation.

    Examples
    --------

    For development environment:

    .. code-block:: bash

        DATA_RENTGEN__SERVER__CORS__ENABLED=True
        DATA_RENTGEN__SERVER__CORS__ALLOW_ORIGINS="*"
        DATA_RENTGEN__SERVER__CORS__ALLOW_METHODS="*"
        DATA_RENTGEN__SERVER__CORS__ALLOW_HEADERS="*"
        DATA_RENTGEN__SERVER__CORS__EXPOSE_HEADERS="X-Request-ID,Location,Access-Control-Allow-Credentials"

    For production environment:

    .. code-block:: bash

        DATA_RENTGEN__SERVER__CORS__ENABLED=True
        DATA_RENTGEN__SERVER__CORS__ALLOW_ORIGINS="production.example.com"
        DATA_RENTGEN__SERVER__CORS__ALLOW_METHODS="GET"
        DATA_RENTGEN__SERVER__CORS__ALLOW_HEADERS="X-Request-ID,X-Request-With"
        DATA_RENTGEN__SERVER__CORS__EXPOSE_HEADERS="X-Request-ID"
        # custom option passed directly to middleware
        DATA_RENTGEN__SERVER__CORS__MAX_AGE=600
    """

    enabled: bool = Field(default=True, description="Set to ``True`` to enable middleware")
    allow_origins: list[str] = Field(default_factory=list, description="Domains allowed for CORS")
    allow_credentials: bool = Field(
        default=False,
        description="If ``True``, cookies should be supported for cross-origin request",
    )
    allow_methods: list[str] = Field(default=["GET", "POST"], description="HTTP Methods allowed for CORS")
    # https://github.com/snok/asgi-correlation-id#cors
    allow_headers: list[str] = Field(
        default=["X-Request-ID", "X-Request-With", "Access-Control-Allow-Origin"],
        description="HTTP headers allowed for CORS",
    )
    expose_headers: list[str] = Field(default=["X-Request-ID"], description="HTTP headers exposed from backend")

    @field_validator("allow_origins", "allow_methods", "allow_headers", "expose_headers", mode="before")
    @classmethod
    def _validate_bootstrap_servers(cls, value: Any):
        if not isinstance(value, str):
            return value
        if "[" in value:
            return json.loads(value)
        return [item.strip() for item in value.split(",")]

    model_config = ConfigDict(extra="allow")
