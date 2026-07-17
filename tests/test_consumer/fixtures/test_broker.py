from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest_asyncio
from faststream.kafka import KafkaBroker, KafkaMessage, TestKafkaBroker

from data_rentgen.consumer import broker_factory

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from data_rentgen.consumer.settings import ConsumerApplicationSettings


@pytest_asyncio.fixture
async def broker(consumer_app_settings: ConsumerApplicationSettings) -> KafkaBroker:
    return broker_factory(settings=consumer_app_settings)


@pytest_asyncio.fixture
async def malformed_handler_with_messages(
    broker: KafkaBroker,
    consumer_app_settings: ConsumerApplicationSettings,
):
    messages = []

    @broker.subscriber(consumer_app_settings.producer.malformed_topic)
    async def handler(message: KafkaMessage):
        messages.append(message)

    return handler, messages


@pytest_asyncio.fixture
async def test_broker(
    broker: KafkaBroker,
    malformed_handler_with_messages: Any,
) -> AsyncGenerator[KafkaBroker, None]:
    async with TestKafkaBroker(broker) as result:
        yield result
