# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LoggingSettings(BaseModel):
    """Data.Rentgen backend logging Settings.

    Examples
    --------

    Using `json` preset:

    ```yaml title="config.yml"
    logging:
      setup: true
      preset: json
    ```
    Passing custom logging config file:

    ```yaml title="config.yml"
    logging:
      setup: true
      custom_config_path: /some/logging.yml
    ```
    Setup logging in some other way, e.g. using [uvicorn args](https://www.uvicorn.org/settings/#logging):

    ```yaml title="config.yml"
    logging:
      setup: false
    ```

    ```bash
    python -m data_rentgen.server --log-level debug
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

    model_config = ConfigDict(extra="forbid")
