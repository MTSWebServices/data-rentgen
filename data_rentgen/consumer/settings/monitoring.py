# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, Field


class MonitoringSettings(BaseModel):
    """Monitoring Settings.

    See
    [FastStream Prometheus](https://faststream.ag2.ai/latest/getting-started/observability/prometheus/) documentation.

    Examples
    --------

    ```bash
    DATA_RENTGEN__MONITORING__ENABLED=True
    DATA_RENTGEN__MONITORING__APP_NAME=data-rentgen-consumer
    ```
    """

    enabled: bool = Field(default=True, description="Set to `True` to enable middleware")
    app_name: str = Field(
        default="data-rentgen-consumer",
        description="Application name, added to all metrics as `app_name` label",
    )
