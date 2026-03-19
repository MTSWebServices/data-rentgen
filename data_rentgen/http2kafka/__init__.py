# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI
from faststream._internal._compat import ExceptionGroup
from faststream.kafka import KafkaBroker
from faststream.kafka.publisher import DefaultPublisher
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.db.factory import session_generator
from data_rentgen.http2kafka.router import router as openlineage_router
from data_rentgen.http2kafka.settings import Http2KafkaApplicationSettings
from data_rentgen.logging.setup_logging import setup_logging
from data_rentgen.server.api.handlers import apply_exception_handlers
from data_rentgen.server.api.monitoring import router as monitoring_router
from data_rentgen.server.middlewares import apply_middlewares
from data_rentgen.server.providers.auth.personal_token_provider import (
    PersonalTokenAuthProvider,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings: Http2KafkaApplicationSettings = app.state.settings
    broker: KafkaBroker = app.state.broker
    await settings.kafka.security.initialize()
    async with broker, anyio.create_task_group() as tg:
        try:
            tg.start_soon(settings.kafka.security.refresh)

            yield

            await settings.kafka.security.destroy()
            tg.cancel_scope.cancel()
        except ExceptionGroup as e:
            for exception in e.exceptions:  # type: ignore[attr-defined]
                raise exception from None


def broker_factory(settings: Http2KafkaApplicationSettings) -> KafkaBroker:
    return KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=settings.kafka.security.to_security(),
        compression_type=settings.kafka.compression.value if settings.kafka.compression else None,
        client_id=f"data-rentgen-{data_rentgen.__version__}",
        logger=logger,
        **settings.kafka.security.extra_broker_kwargs(),
        **settings.kafka.model_dump(exclude={"bootstrap_servers", "security", "compression"}),
        **settings.producer.model_dump(exclude={"main_topic", "malformed_topic"}),
    )


def application_factory(settings: Http2KafkaApplicationSettings) -> FastAPI:
    application = FastAPI(
        title="Data.Rentgen Http2Kafka",
        description="Proxies all received OpenLineage events and sends them to Kafka",
        version=data_rentgen.__version__,
        debug=settings.server.debug,
        # will be set up by middlewares
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    application.state.settings = settings
    application.include_router(monitoring_router)
    application.include_router(openlineage_router)

    PersonalTokenAuthProvider.setup(application)

    # Reusing Server source code
    apply_exception_handlers(application)
    apply_middlewares(application, settings.server)

    broker = broker_factory(settings)
    application.state.broker = broker
    publisher = broker.publisher(settings.producer.main_topic)

    # if dependency is a sync function, FastAPI runs it using asyncio.to_thread
    async def get_settings():
        return settings

    async def get_publisher():
        return publisher

    application.dependency_overrides.update(
        {
            Http2KafkaApplicationSettings: get_settings,
            DefaultPublisher: get_publisher,
            AsyncSession: session_generator(settings.database),  # type: ignore[dict-item]
        },
    )
    return application


def get_application():
    settings = Http2KafkaApplicationSettings()
    setup_logging(settings.logging)
    logger.info("Starting Data.Rentgen HTTP2Kafka with settings:\n%r", settings)
    return application_factory(settings=settings)
