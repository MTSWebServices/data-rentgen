# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.server.settings.auth import AuthSettings
from data_rentgen.server.settings.server import ServerSettings
from data_rentgen.settings import BaseSettings


class ServerApplicationSettings(BaseSettings):
    """Data.Rentgen REST API settings.

    Application can be configured in 3 ways, in descending order of priority:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.server.application_factory]
    * By storing settings in a `config.yml` configuration file
    * By setting environment variables matching a specific key

    Environment variable names are written in uppercase, prefixed with `DATA_RENTGEN__`,
    and use `__` to delimit nested items.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```yaml title="config.yml"
    database:
      url: postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen
    logging:
      preset: json
    server:
      debug: true
    ```
    """  # noqa: E501

    auth: AuthSettings = Field(
        default_factory=AuthSettings,
        description="Auth settings",
    )
    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore[arg-type]
        description="[Database settings][configuration-database]",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="[Logging settings][configuration-server-logging]",
    )
    server: ServerSettings = Field(
        default_factory=ServerSettings,
        description="[Server settings][configuration-server]",
    )
