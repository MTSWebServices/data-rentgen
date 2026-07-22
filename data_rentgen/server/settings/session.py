# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import textwrap

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationInfo, field_validator

DEFAULT_MAX_AGE = 1_209_600


class SessionSettings(BaseModel):
    """Session Middleware Settings.

    Required for [auth-server-keycloak][].

    See [SessionMiddleware](https://www.starlette.io/middleware/#sessionmiddleware) documentation.

    !!! note

        You can pass here any extra option supported by `SessionMiddleware`,
        even if it is not mentioned in documentation.

    Examples
    --------

    ```yaml title="config.yml"
    server:
      session:
        enabled: true
        secret_key: secret
        session_cookie: custom_cookie_name
        max_age: null  # cookie will last as long as the browser session
        same_site: strict
        https_only: true
        domain: example.com
    ```
    """

    enabled: bool = Field(
        default=True,
        description="Set to `True` to enable SessionMiddleware",
    )
    secret_key: SecretStr | None = Field(
        default=None,
        description=textwrap.dedent(
            """
            Secret key for encrypting cookies.

            Can be any string. It is recommended to generate random value for every application instance, e.g.:

            ```shell
            pwgen 32 1
            ```
            """,
        ),
    )
    session_cookie: str | None = Field(
        default="session",
        description="Name of the session cookie. Change this if there are multiple application under the same domain.",
    )
    max_age: int | None = Field(
        default=DEFAULT_MAX_AGE,
        description="Session expiry time in seconds. Defaults to 2 weeks.",
    )
    same_site: str | None = Field(
        default="lax",
        description="Prevents cookie from being sent with cross-site requests.",
    )
    path: str | None = Field(default="/", description="Path to restrict session cookie access.")
    https_only: bool = Field(default=False, description="Secure flag for HTTPS-only access.")
    domain: str | None = Field(
        default=None,
        description="Domain for sharing cookies between subdomains or cross-domains.",
    )

    model_config = ConfigDict(extra="allow")

    @field_validator("secret_key", mode="after")
    @classmethod
    def _validate_secret_key(cls, value: SecretStr | None, info: ValidationInfo) -> SecretStr | None:
        if not value and info.data.get("enabled"):
            msg = "secret_key is required"
            raise ValueError(msg)
        return value
