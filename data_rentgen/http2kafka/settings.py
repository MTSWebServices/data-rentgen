# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.settings.kafka import KafkaSettings
from data_rentgen.consumer.settings.producer import ProducerSettings
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.server.settings.auth import AuthSettings
from data_rentgen.server.settings.server import ServerSettings
from data_rentgen.settings import BaseSettings


class Http2KafkaApplicationSettings(BaseSettings):
    """Data.Rentgen Http2Kafka settings.

    Application can be configured in 3 ways, in descending order of priority:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.http2kafka.application_factory]
    * By storing settings in a `config.yml` configuration file
    * By setting environment variables matching a specific key

    Environment variable names are written in uppercase, prefixed with `DATA_RENTGEN__`,
    and use `__` to delimit nested items.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```yaml title="config.yml"
    logging:
      preset: json
    server:
      debug: true
    kafka:
      bootstrap_servers: [localhost:9092]
    producer:
      main_topic: input.runs
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
