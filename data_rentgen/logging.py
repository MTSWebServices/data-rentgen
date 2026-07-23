# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import logging.config

from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from pydantic_settings_logging import (
    FilterConfig,
    FormatterConfig,
    HandlerConfig,
    LoggerConfig,
    RootLoggerConfig,
    StreamHandlerConfig,
)
from pydantic_settings_logging import (
    LoggingSettings as BaseLoggingSettings,
)

__all__ = [
    "DEFAULT_LOGGING_SETTINGS",
    "LoggingSettings",
    "setup_logging",
]


# https://github.com/vduseev/pydantic-settings-logging/pull/1
class CallableFactoryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    callable: str = Field(
        description="Custom callable",
        validation_alias=AliasChoices("callable", "()"),
        serialization_alias="()",
    )


class LoggingSettings(BaseLoggingSettings):
    """Python logging configuration.

    See [logging.config](https://docs.python.org/3/library/logging.config.html#dictionary-schema-details) docs.

    Logging to `stdout` with colored text:

    ```yaml title="config.yml"
    logging:
        # development usage only
        version: 1
        disable_existing_loggers: false

        filters:
            # Add request ID as extra field named `correlation_id` to each log record.
            # This is used in combination with settings.server.request_id.enabled=True
            # See https://github.com/snok/asgi-correlation-id#configure-logging
            correlation_id:
                (): asgi_correlation_id.CorrelationIdFilter
                uuid_length: 32
                default_value: '-'

        formatters:
            colored_with_correlation:
                (): coloredlogs.ColoredFormatter
                # Add correlation_id to log records
                fmt: '%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d [%(levelname)s] %(message)s %(correlation_id)s'
                datefmt: '%Y-%m-%d %H:%M:%S'
            colored:
                (): coloredlogs.ColoredFormatter
                fmt: '%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d [%(levelname)s] %(message)s'
                datefmt: '%Y-%m-%d %H:%M:%S'

        handlers:
            main:
                class: logging.StreamHandler
                formatter: colored_with_correlation
                filters: [correlation_id]
                stream: ext://sys.stdout
            faststream:
                class: logging.StreamHandler
                formatter: colored
                filters: []
                stream: ext://sys.stdout

        loggers:
            '':
                handlers: [main]
                level: INFO
                propagate: false
            uvicorn:
                handlers: [main]
                level: INFO
                propagate: false
            data_rentgen.consumer:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka.conn:
                handlers: [faststream]
                level: ERROR
                propagate: false
    ```
    Logging to `stdout` without colored text:

    ```yaml title="config.yml"
    logging:
        # development usage only
        version: 1
        disable_existing_loggers: false

        filters:
            # Add request ID as extra field named `correlation_id` to each log record.
            # This is used in combination with settings.server.request_id.enabled=True
            # See https://github.com/snok/asgi-correlation-id#configure-logging
            correlation_id:
                (): asgi_correlation_id.CorrelationIdFilter
                uuid_length: 32
                default_value: '-'

        formatters:
            plain_with_correlation:
                (): logging.Formatter
                # Add correlation_id to log records
                fmt: '%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d [%(levelname)s] %(correlation_id)s %(message)s'
                datefmt: '%Y-%m-%d %H:%M:%S'
            plain:
                (): logging.Formatter
                fmt: '%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d [%(levelname)s] %(message)s'
                datefmt: '%Y-%m-%d %H:%M:%S'

        handlers:
            main:
                class: logging.StreamHandler
                formatter: plain_with_correlation
                filters: [correlation_id]
                stream: ext://sys.stdout
            faststream:
                class: logging.StreamHandler
                formatter: plain
                filters: []
                stream: ext://sys.stdout

        loggers:
            '':
                handlers: [main]
                level: INFO
                propagate: false
            uvicorn:
                handlers: [main]
                level: INFO
                propagate: false
            data_rentgen.consumer:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka.conn:
                handlers: [faststream]
                level: ERROR
                propagate: false
    ```
    Logging to `stdout` in JSON format:

    ```yaml title="config.yml"
    logging:
        version: 1
        disable_existing_loggers: false

        filters:
            # Add request ID as extra field named `correlation_id` to each log record.
            # This is used in combination with settings.server.request_id.enabled=True
            # See https://github.com/snok/asgi-correlation-id#configure-logging
            correlation_id:
                (): asgi_correlation_id.CorrelationIdFilter
                uuid_length: 32
                default_value: '-'

        formatters:
            json_with_correlation:
                (): pythonjsonlogger.jsonlogger.JsonFormatter
                # Add correlation_id to log records
                fmt: '%(processName)s %(process)d %(threadName)s %(thread)d %(name)s %(lineno)d %(levelname)s %(message)s %(correlation_id)s'
                timestamp: true
            json:
                (): pythonjsonlogger.jsonlogger.JsonFormatter
                fmt: '%(processName)s:%(process)d %(name)s:%(lineno)d [%(levelname)s] %(message)s'
                timestamp: true

        handlers:
            main:
                class: logging.StreamHandler
                formatter: json_with_correlation
                filters: [correlation_id]
                stream: ext://sys.stdout
            faststream:
                class: logging.StreamHandler
                formatter: json
                filters: []
                stream: ext://sys.stdout

        loggers:
            '':
                handlers: [main]
                level: INFO
                propagate: false
            uvicorn:
                handlers: [main]
                level: INFO
                propagate: false
            data_rentgen.consumer:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka:
                handlers: [faststream]
                level: INFO
                propagate: false
            aiokafka.conn:
                handlers: [faststream]
                level: ERROR
                propagate: false
    ```
    """  # noqa: E501

    filters: dict[str, FilterConfig | CallableFactoryConfig] = Field(
        default_factory=dict,
        description="Logging filters",
    )
    formatters: dict[str, FormatterConfig | CallableFactoryConfig] = Field(
        default_factory=dict,
        description="Logging formatters",
    )
    handlers: dict[str, HandlerConfig | CallableFactoryConfig] = Field(
        default_factory=dict,
        description="Logging handlers",
    )


DEFAULT_LOGGING_SETTINGS = LoggingSettings(
    disable_existing_loggers=False,
    filters={
        # Add request ID as extra field named `correlation_id` to each log record.
        # This is used in combination with settings.server.request_id.enabled=True
        # See https://github.com/snok/asgi-correlation-id#configure-logging
        "correlation_id": CallableFactoryConfig(
            callable="asgi_correlation_id.CorrelationIdFilter",
            uuid_length=32,  # type: ignore[call-arg]
            default_value="-",  # type: ignore[call-arg]
        ),
    },
    formatters={
        "colored": FormatterConfig(
            class_="coloredlogs.ColoredFormatter",
            format=(
                "%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d "
                "[%(levelname)s] %(correlation_id)s %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
        "faststream": FormatterConfig(
            class_="coloredlogs.ColoredFormatter",
            format=(
                "%(asctime)s.%(msecs)03d %(processName)s:%(process)d %(name)s:%(lineno)d[%(levelname)s] %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        ),
    },
    handlers={
        "main": StreamHandlerConfig(
            formatter="colored",
            filters=["correlation_id"],
            stream="ext://sys.stdout",
        ),
        "faststream": StreamHandlerConfig(
            formatter="faststream",
            filters=[],
            stream="ext://sys.stdout",
        ),
    },
    root=RootLoggerConfig(
        handlers=["main"],
        level="INFO",
    ),
    loggers={
        "uvicorn": LoggerConfig(
            handlers=["main"],
            level="INFO",
            propagate=False,
        ),
        "data_rengen.consumer": LoggerConfig(
            handlers=["faststream"],
            level="INFO",
            propagate=False,
        ),
        "aiokafka": LoggerConfig(
            handlers=["faststream"],
            level="INFO",
            propagate=False,
        ),
        "aiokafka.conn": LoggerConfig(
            handlers=["faststream"],
            level="ERROR",
            propagate=False,
        ),
    },
)


def setup_logging(settings: LoggingSettings):
    logging.config.dictConfig(settings.model_dump())
