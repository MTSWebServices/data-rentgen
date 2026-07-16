# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel, ConfigDict, Field


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
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="""Custom labels added to all metrics, e.g. `{"instance": "production"}`""",
        serialization_alias="custom_labels",
    )
    received_messages_size_buckets: list[float] = Field(
        default_factory=list,
        description="List of buckets for received messages size histogram",
    )

    model_config = ConfigDict(extra="ignore")
