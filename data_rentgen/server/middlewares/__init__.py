# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from fastapi import FastAPI

from data_rentgen.server.middlewares.application_version import (
    apply_application_version_middleware,
)
from data_rentgen.server.middlewares.cors import apply_cors_middleware
from data_rentgen.server.middlewares.monitoring import (
    apply_monitoring_metrics_middleware,
)
from data_rentgen.server.middlewares.openapi import apply_openapi_middleware
from data_rentgen.server.middlewares.request_id import apply_request_id_middleware
from data_rentgen.server.middlewares.static_files import apply_static_files
from data_rentgen.server.settings import ServerSettings


def apply_middlewares(
    application: FastAPI,
    settings: ServerSettings,
) -> FastAPI:
    """Add middlewares to the application."""

    apply_cors_middleware(application, settings.cors)
    apply_monitoring_metrics_middleware(application, settings.monitoring)
    apply_request_id_middleware(application, settings.request_id)
    apply_application_version_middleware(application, settings.application_version)
    apply_openapi_middleware(application, settings.openapi)
    apply_static_files(application, settings.static_files)

    return application
