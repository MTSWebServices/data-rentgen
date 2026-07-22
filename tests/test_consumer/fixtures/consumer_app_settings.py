import pytest

from data_rentgen.consumer.settings import ConsumerApplicationSettings


@pytest.fixture(scope="session", params=[{}])
def consumer_app_settings(request: pytest.FixtureRequest) -> ConsumerApplicationSettings:
    return ConsumerApplicationSettings(**request.param)
