# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_rentgen.consumer.settings.consumer import ConsumerSettings
from data_rentgen.consumer.settings.kafka import KafkaSettings
from data_rentgen.consumer.settings.producer import ProducerSettings
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings


class ConsumerApplicationSettings(BaseSettings):
    """Data.Rentgen Kafka consumer settings.

    Application can be configured in 2 ways:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.consumer.application_factory]
    * By setting up environment variables matching a specific key.

      All environment variable names are written in uppercase and should be prefixed with `DATA_RENTGEN__`.
      Nested items are delimited with `__`.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```bash
    # same as settings.database.url = "postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen"
    DATA_RENTGEN__DATABASE__URL=postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen

    # same as settings.kafka.bootstrap_servers = ["kafka1:9092", "kafka2:9092"]
    DATA_RENTGEN__KAFKA__BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092"

    # same as settings.logging.preset = "json"
    DATA_RENTGEN__LOGGING__PRESET=json
    ```
    """  # noqa: E501

    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,  # type: ignore[arg-type]
        description="[Database settings][configuration-database]",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="[Logging settings][configuration-consumer-logging]",
    )
    kafka: KafkaSettings = Field(
        description="[Kafka settings][configuration-consumer-kafka]",
    )
    consumer: ConsumerSettings = Field(
        default_factory=ConsumerSettings,
        description="[Consumer settings][configuration-consumer-specific]",
    )
    producer: ProducerSettings = Field(
        default_factory=ProducerSettings,
        description="[Producer settings][configuration-producer-specific]",
    )

    model_config = SettingsConfigDict(env_prefix="DATA_RENTGEN__", env_nested_delimiter="__", extra="forbid")
