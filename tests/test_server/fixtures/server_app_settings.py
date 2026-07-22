import pytest

from data_rentgen.server.settings import ServerApplicationSettings


@pytest.fixture(scope="session", params=[{}])
def server_app_settings(request: pytest.FixtureRequest) -> ServerApplicationSettings:
    return ServerApplicationSettings(**request.param)
