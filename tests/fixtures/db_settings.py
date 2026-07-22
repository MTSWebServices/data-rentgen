import pytest

from data_rentgen.db.settings import DatabaseApplicationSettings, DatabaseSettings


@pytest.fixture(scope="session", params=[{}])
def db_settings(request: pytest.FixtureRequest) -> DatabaseSettings:
    settings = DatabaseApplicationSettings()
    return DatabaseSettings.model_validate({**settings.database.model_dump(), **request.param})
