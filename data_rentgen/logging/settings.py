# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    """Data.Rentgen backend logging Settings.

    Examples
    --------

    Using `json` preset:

    ```bash
    DATA_RENTGEN__LOGGING__SETUP=True
    DATA_RENTGEN__LOGGING__PRESET=json
    ```
    Passing custom logging config file:

    ```bash
    DATA_RENTGEN__LOGGING__SETUP=True
    DATA_RENTGEN__LOGGING__CUSTOM_CONFIG_PATH=/some/logging.yml
    ```
    Setup logging in some other way, e.g. using [uvicorn args](https://www.uvicorn.org/settings/#logging):

    ```bash
    $ export DATA_RENTGEN__LOGGING__SETUP=False
    $ python -m data_rentgen.server --log-level debug
    ```
    """

    setup: bool = Field(
        default=True,
        description="If `True`, setup logging during application start",
    )
    preset: Literal["json", "plain", "colored"] = Field(
        default="plain",
        description=textwrap.dedent(
            """
            Name of logging preset to use.

            There are few logging presets bundled to `data-rentgen[server]` package:

            ??? note "`plain` preset"

                This preset is recommended to use in environment which do not support colored output,
                e.g. CI jobs

                ```yaml
                --8<-- "data_rentgen/logging/presets/plain.yml"
                ```

            ??? note "`colored` preset"

                This preset is recommended to use in development environment,
                as it simplifies debugging. Each log record is output with color specific for a log level

                ```yaml
                --8<-- "data_rentgen/logging/presets/colored.yml"
                ```

            ??? note "`json` preset"

                This preset is recommended to use in production environment,
                as it allows to avoid writing complex log parsing configs. Each log record is output as JSON line

                ```yaml
                --8<-- "data_rentgen/logging/presets/json.yml"
                ```
            """,
        ),
    )

    custom_config_path: Path | None = Field(
        default=None,
        description=textwrap.dedent(
            """
            Path to custom logging configuration file. If set, overrides [preset][] value.

            File content should be in YAML format and conform
            [logging.dictConfig](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema).
            """,
        ),
    )

    model_config = SettingsConfigDict(env_prefix="DATA_RENTGEN__LOGGING__", env_nested_delimiter="__", extra="forbid")
