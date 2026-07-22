import pytest

from data_rentgen.http2kafka.settings import Http2KafkaApplicationSettings


@pytest.fixture(scope="session", params=[{}])
def http2kafka_app_settings(request: pytest.FixtureRequest) -> Http2KafkaApplicationSettings:
    return Http2KafkaApplicationSettings(**request.param)
