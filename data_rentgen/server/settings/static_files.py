# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class StaticFilesSettings(BaseModel):
    """Static files serving settings.

    Files are served at `/static` endpoint.

    Examples
    --------

    ```bash
    DATA_RENTGEN__SERVER__STATIC_FILES__ENABLED=True
    DATA_RENTGEN__SERVER__STATIC_FILES__DIRECTORY=/app/data_rentgen/server/static
    ```
    """

    enabled: bool = Field(default=True, description="Set to `True` to enable static file serving")
    directory: Path = Field(
        default=Path("mddocs/docs/_static"),
        description="Directory containing static files",
    )

    @field_validator("directory")
    def _validate_directory(cls, value: Path) -> Path:  # noqa: N805
        if not value.exists():
            msg = f"Directory '{value}' does not exist"
            raise ValueError(msg)
        if not value.is_dir():
            msg = f"Path '{value}' is not a directory"
            raise ValueError(msg)
        return value
