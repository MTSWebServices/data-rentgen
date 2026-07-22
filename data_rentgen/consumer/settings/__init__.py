# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pydantic import Field

from data_rentgen.consumer.settings.consumer import ConsumerSettings
from data_rentgen.consumer.settings.kafka import KafkaSettings
from data_rentgen.consumer.settings.monitoring import MonitoringSettings
from data_rentgen.consumer.settings.producer import ProducerSettings
from data_rentgen.db.settings import DatabaseSettings
from data_rentgen.logging.settings import LoggingSettings
from data_rentgen.settings import BaseSettings


class ConsumerApplicationSettings(BaseSettings):
    """Data.Rentgen Kafka consumer settings.

    Application can be configured in 3 ways, in descending order of priority:

    * By explicitly passing `settings` object as an argument to [application_factory][data_rentgen.consumer.application_factory]
    * By storing settings in a `config.yml` configuration file
    * By setting environment variables matching a specific key

    Environment variable names are written in uppercase, prefixed with `DATA_RENTGEN__`,
    and use `__` to delimit nested items.

    More details can be found in [Pydantic documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

    Examples
    --------

    ```yaml title="config.yml"
    database:
      url: postgresql+asyncpg://postgres:postgres@localhost:5432/data_rentgen
    kafka:
      bootstrap_servers: [kafka1:9092, kafka2:9092]
    logging:
      preset: json
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
    monitoring: MonitoringSettings = Field(
        default_factory=MonitoringSettings,
        description="[Monitoring settings][configuration-consumer-monitoring]",
    )
