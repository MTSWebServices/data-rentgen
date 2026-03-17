# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import asynccontextmanager
from typing import Any

import anyio
from fast_depends import dependency_provider
from faststream import ContextRepo, FastStream
from faststream._internal._compat import ExceptionGroup
from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.kafka import KafkaBroker
from faststream.kafka.publisher import DefaultPublisher
from faststream.kafka.subscriber.usecase import BatchSubscriber
from faststream.specification.asyncapi import AsyncAPI
from sqlalchemy.ext.asyncio import AsyncSession

import data_rentgen
from data_rentgen.consumer.settings import ConsumerApplicationSettings
from data_rentgen.consumer.subscribers import runs_events_subscriber
from data_rentgen.db.factory import session_generator
from data_rentgen.logging.setup_logging import setup_logging

logger = logging.getLogger(__name__)


@get  # type: ignore[arg-type]
async def liveness(scope: dict[str, Any]) -> AsgiResponse:
    return AsgiResponse(b"", status_code=204)


def broker_factory(settings: ConsumerApplicationSettings) -> KafkaBroker:
    broker = KafkaBroker(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        security=settings.kafka.security.to_security(),
        compression_type=settings.kafka.compression.value if settings.kafka.compression else None,
        client_id=f"data-rentgen-{data_rentgen.__version__}",
        logger=logger,
        **settings.kafka.security.extra_broker_kwargs(),
        **settings.kafka.model_dump(exclude={"bootstrap_servers", "security", "compression"}),
        **settings.producer.model_dump(exclude={"main_topic", "malformed_topic"}),
    )

    # register subscribers using settings
    consumer_settings = settings.consumer.model_dump(exclude={"topics_list", "topics_pattern", "malformed_topic"})

    subscribe = broker.subscriber(
        *settings.consumer.topics_list,
        pattern=settings.consumer.topics_pattern,
        **consumer_settings,
        batch=True,
        # Disable parsing JSONs on FastStream level
        decoder=lambda _: None,
    )

    # register subscriber
    batch_subscriber = subscribe(runs_events_subscriber)

    async def get_subscriber():
        return batch_subscriber

    # FastStream uses WeakSet for subscribers, so we need to keep long lived reference somewhere
    dependency_provider.override(BatchSubscriber, get_subscriber)

    # register publisher
    publisher = broker.publisher(settings.producer.malformed_topic)

    async def get_publisher():
        return publisher

    dependency_provider.override(DefaultPublisher, get_publisher)

    # Override session generator
    dependency_provider.override(AsyncSession, session_generator(settings.database))
    return broker


def application_factory(settings: ConsumerApplicationSettings) -> AsgiFastStream:
    @asynccontextmanager
    async def security_lifespan(context: ContextRepo):
        try:
            await settings.kafka.security.initialize()
            async with anyio.create_task_group() as tg:
                tg.start_soon(settings.kafka.security.refresh)

                yield

                await settings.kafka.security.destroy()
                tg.cancel_scope.cancel()
        except ExceptionGroup as e:
            for exception in e.exceptions:  # type: ignore[attr-defined]
                raise exception from None

    return FastStream(
        broker_factory(settings),
        lifespan=security_lifespan,
        specification=AsyncAPI(
            title="Data.Rentgen",
            description="Data.Rentgen is a nextgen DataLineage service",
            version=data_rentgen.__version__,
        ),
        logger=logger,
    ).as_asgi(asgi_routes=[("/monitoring/ping", liveness)])


def get_application():
    settings = ConsumerApplicationSettings()
    setup_logging(settings.logging)
    logger.info("Starting Data.Rentgen consumer with settings:\n%r", settings)
    return application_factory(settings=settings)
