# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import BaseModel, ConfigDict, Field

from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.settings import BaseSettings


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

    url: str = Field(
        description=textwrap.dedent(
            """
            Database connection URL.

            See [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls)

            !!! warning

                Only async drivers are supported, e.g. `asyncpg`
            """,
        ),
    )

    model_config = ConfigDict(extra="allow")


class DatabaseApplicationSettings(BaseSettings):
    """Settings used by database migrations and maintenance scripts."""

    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore[arg-type]
        description="Database settings",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="Logging settings",
    )
