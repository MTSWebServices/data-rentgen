# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import textwrap

from pydantic import BaseModel, ConfigDict, Field


class MonitoringSettings(BaseModel):
    """Monitoring Settings.

    See [starlette-exporter](https://github.com/stephenhillier/starlette_exporter#options) documentation.

    !!! note

        You can pass here any extra option supported by `starlette-exporter`,
        even if it is not mentioned in documentation.

    Examples
    --------

    ```bash
    DATA_RENTGEN__SERVER__MONITORING__ENABLED=True
    DATA_RENTGEN__SERVER__MONITORING__SKIP_PATHS=["/some/path"]
    DATA_RENTGEN__SERVER__MONITORING__SKIP_METHODS=["OPTIONS"]
    ```
    """

    enabled: bool = Field(default=True, description="Set to `True` to enable middleware")
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="""Custom labels added to all metrics, e.g. `{"instance": "production"}`""",
    )
    skip_paths: set[str] = Field(
        default_factory=set,
        description="Custom paths should be skipped from metrics, like `/some/endpoint`",
    )
    skip_methods: set[str] = Field(
        default={"OPTIONS"},
        description="HTTP methods which should be excluded from metrics",
    )
    # These will be new defaults in starlette-exporter 0.18:
    # https://github.com/stephenhillier/starlette_exporter/issues/79
    group_paths: bool = Field(
        default=True,
        description=textwrap.dedent(
            """
            If `True` (recommended), add request path to metrics literally as described
            in OpenAPI schema, e.g. `/namespaces/{id}`, without substitution with path real values.

            If `False`, all real request paths to metrics, e.g. `/namespaces/123`.
            """,
        ),
    )
    filter_unhandled_paths: bool = Field(
        default=True,
        description=textwrap.dedent(
            """
            If `True`, add metrics for paths only mentioned in OpenAPI schema.

            If `False`, add all requested paths to metrics.
            """,
        ),
    )

    model_config = ConfigDict(extra="allow")
