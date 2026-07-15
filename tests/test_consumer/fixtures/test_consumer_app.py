from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from data_rentgen.consumer import application_factory

if TYPE_CHECKING:
    from faststream.asgi import AsgiFastStream

    from data_rentgen.consumer.settings import ConsumerApplicationSettings


@pytest.fixture(scope="session")
def test_consumer_app(consumer_app_settings: ConsumerApplicationSettings) -> AsgiFastStream:
    return application_factory(settings=consumer_app_settings)
