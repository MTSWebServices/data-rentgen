# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import TYPE_CHECKING

from fastapi import FastAPI

import data_rentgen
from data_rentgen.db.factory import create_session_factory
from data_rentgen.logging import setup_logging
from data_rentgen.server.api.handlers import apply_exception_handlers
from data_rentgen.server.api.router import api_router
from data_rentgen.server.middlewares import apply_middlewares
from data_rentgen.server.providers.auth.personal_token_provider import PersonalTokenAuthProvider
from data_rentgen.server.settings import ServerApplicationSettings

if TYPE_CHECKING:
    from data_rentgen.server.providers.auth import AuthProvider

__all__ = [
    "ServerApplicationSettings",
    "application_factory",
    "get_application",
]

logger = logging.getLogger(__name__)


def application_factory(settings: ServerApplicationSettings) -> FastAPI:
    application = FastAPI(
        title="Data.Rentgen Server",
        description="Provides REST API for fetching lineage data",
        version=data_rentgen.__version__,
        debug=settings.server.debug,
        # will be set up by middlewares
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    application.state.settings = settings
    application.include_router(api_router)

    apply_exception_handlers(application)
    auth_class: type[AuthProvider] = settings.auth.provider  # type: ignore[assignment]
    auth_class.setup(application)

    PersonalTokenAuthProvider.setup(application)
    apply_middlewares(application, settings.server)

    application.state.session_factory = create_session_factory(settings.database)
    return application


def get_application():
    settings = ServerApplicationSettings()
    setup_logging(settings.logging)
    logger.info("Starting Data.Rentgen server with settings:\n%r", settings)
    return application_factory(settings=settings)
