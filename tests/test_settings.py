import os
from textwrap import dedent

import pytest

from data_rentgen.consumer.settings import ConsumerApplicationSettings
from data_rentgen.db.settings import DatabaseApplicationSettings
from data_rentgen.http2kafka.settings import Http2KafkaApplicationSettings
from data_rentgen.server.settings import ServerApplicationSettings


def _clear_settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for variable_name in tuple(os.environ):
        if variable_name.startswith("DATA_RENTGEN__"):
            monkeypatch.delenv(variable_name)


def test_all_application_settings_are_loaded_from_default_yaml_file(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_settings_environment(monkeypatch)
    monkeypatch.delenv("DATA_RENTGEN_CONFIG_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        dedent(
            """\
            database:
              url: postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen
            kafka:
              bootstrap_servers: [yaml:9092]
            server:
              debug: true
            auth:
              personal_tokens:
                enabled: false
            """,
        ),
        encoding="utf-8",
    )

    server_settings = ServerApplicationSettings()
    consumer_settings = ConsumerApplicationSettings()
    http2kafka_settings = Http2KafkaApplicationSettings()
    database_settings = DatabaseApplicationSettings()

    assert str(server_settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"
    assert str(consumer_settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"
    assert str(http2kafka_settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"
    assert str(database_settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"
    assert consumer_settings.kafka.bootstrap_servers == ["yaml:9092"]
    assert http2kafka_settings.kafka.bootstrap_servers == ["yaml:9092"]
    assert server_settings.server.debug is True
    assert server_settings.auth.personal_tokens.enabled is False
    assert http2kafka_settings.auth.personal_tokens.enabled is False


def test_yaml_file_overrides_environment(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_settings_environment(monkeypatch)
    config_path = tmp_path / "custom.yml"
    config_path.write_text(
        dedent(
            """\
            database:
              url: postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen
            """,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATA_RENTGEN_CONFIG_FILE", str(config_path))
    monkeypatch.setenv(
        "DATA_RENTGEN__DATABASE__URL",
        "postgresql+asyncpg://env:env@localhost:5432/data_rentgen",
    )

    settings = DatabaseApplicationSettings()

    assert str(settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"


def test_environment_fills_values_missing_from_yaml_file(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_settings_environment(monkeypatch)
    config_path = tmp_path / "custom.yml"
    config_path.write_text(
        dedent(
            """\
            database:
              url: postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen
            """,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATA_RENTGEN_CONFIG_FILE", str(config_path))

    settings = DatabaseApplicationSettings()

    assert str(settings.database.url) == "postgresql+asyncpg://yaml:yaml@localhost:5432/data_rentgen"


def test_settings_can_be_loaded_from_environment_without_yaml_file(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_settings_environment(monkeypatch)
    monkeypatch.setenv("DATA_RENTGEN_CONFIG_FILE", str(tmp_path / "missing.yml"))
    monkeypatch.setenv(
        "DATA_RENTGEN__DATABASE__URL",
        "postgresql+asyncpg://env:env@localhost:5432/data_rentgen",
    )

    settings = DatabaseApplicationSettings()

    assert str(settings.database.url) == "postgresql+asyncpg://env:env@localhost:5432/data_rentgen"
