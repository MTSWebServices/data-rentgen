# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_rentgen.consumer.settings.kafka import KafkaSettings
from data_rentgen.consumer.settings.producer import ProducerSettings
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.server.settings.auth import AuthSettings
from data_rentgen.server.settings.server import ServerSettings


class Http2KafkaApplicationSettings(BaseSettings):
    """Data.Rentgen Http2Kafka settings.

    Application can be configured in 2 ways:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.http2kafka.application_factory]
    * By setting up environment variables matching a specific key.

      All environment variable names are written in uppercase and should be prefixed with `DATA_RENTGEN__`.
      Nested items are delimited with `__`.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```bash
    # same as settings.logging.preset = "json"
    DATA_RENTGEN__LOGGING__PRESET=json

    # same as settings.server.debug = True
    DATA_RENTGEN__SERVER__DEBUG=True

    # same as settings.kafka.bootstrap_servers = ["localhost:9092"]
    DATA_RENTGEN__KAFKA__BOOTSTRAP_SERVERS=["localhost:9092"]

    # same as settings.producer.main_topic = "input.runs"
    DATA_RENTGEN__PRODUCER__MAIN_TOPIC="input.runs"
    ```
    """  # noqa: E501

    auth: AuthSettings = Field(
        default_factory=AuthSettings,
        description="[Authentication settings][auth-server]",
    )
    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore[arg-type]
        description="[Database settings][configuration-database]",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="[Logging settings][configuration-server-logging]",
    )
    server: ServerSettings = Field(
        default_factory=ServerSettings,
        description="[Server settings][configuration-server]",
    )
    kafka: KafkaSettings = Field(
        default_factory=KafkaSettings,  # type: ignore[arg-type]
        description="[Kafka settings][configuration-consumer-kafka]",
    )
    producer: ProducerSettings = Field(
        default_factory=ProducerSettings,
        description="[Producer settings][configuration-producer-specific]",
    )

    model_config = SettingsConfigDict(env_prefix="DATA_RENTGEN__", env_nested_delimiter="__", extra="forbid")
