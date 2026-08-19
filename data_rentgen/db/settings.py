# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, PostgresDsn, UrlConstraints
from sqlalchemy import make_url

from data_rentgen.logging import DEFAULT_LOGGING_SETTINGS, LoggingSettings
from data_rentgen.settings import BaseSettings


def validate_url(value: PostgresDsn):
    url = make_url(str(value))
    if not url.database:
        msg = "Database URL must contain database name"
        raise ValueError(msg)

    if not url.username or not url.password:
        msg = "Database URL must contain username and password"
        raise ValueError(msg)

    return value


PostgresURL = Annotated[
    PostgresDsn,
    UrlConstraints(allowed_schemes=["postgresql+asyncpg", "postgresql+psycopg"], default_port=5432, host_required=True),
    AfterValidator(validate_url),
]


class DatabaseSettings(BaseModel):
    """Data.Rentgen backend database settings.

    !!! note

        You can pass here any extra option supported by
        [SQLAlchemy Engine class](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine),
        even if it is not mentioned in documentation.

    Examples
    --------

    ```yaml title="config.yml"
    database:
      url: postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen
      # custom option passed directly to engine factory
      pool_pre_ping: true
    ```
    """

    url: PostgresURL = Field(
        description=textwrap.dedent(
            """
            Database connection URL.

            Mandatory components:

            * host
            * username (urlencoded)
            * password (urlencoded)

            See [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls)

            !!! warning

                Only async drivers are supported, e.g. `asyncpg` or `psycopg`
            """,
        ),
    )

    model_config = ConfigDict(extra="allow")

    def __repr_args__(self):
        safe_url = make_url(str(self.url)).render_as_string(
            hide_password=True,
        )
        extra = super().__repr_args__()
        return [
            ("url", safe_url),
            *[item for item in extra if item[0] != "url"],
        ]


class DatabaseApplicationSettings(BaseSettings):
    """Settings used by database migrations and maintenance scripts."""

    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore[arg-type]
        description="Database settings",
    )
    logging: LoggingSettings = Field(
        default=DEFAULT_LOGGING_SETTINGS,
        description="Logging settings",
    )
