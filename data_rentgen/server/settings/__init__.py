# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.server.settings.auth import AuthSettings
from data_rentgen.server.settings.server import ServerSettings


class ServerApplicationSettings(BaseSettings):
    """Data.Rentgen REST API settings.

    Application can be configured in 2 ways:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.server.application_factory]
    * By setting up environment variables matching a specific key.

      All environment variable names are written in uppercase and should be prefixed with `DATA_RENTGEN__`.
      Nested items are delimited with `__`.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```bash
    # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen"
    DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen

    # same as settings.logging.preset = "json"
    DATA_RENTGEN__LOGGING__PRESET=json

    # same as settings.server.debug = True
    DATA_RENTGEN__SERVER__DEBUG=True
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

    model_config = SettingsConfigDict(env_prefix="DATA_RENTGEN__", env_nested_delimiter="__", extra="forbid")
