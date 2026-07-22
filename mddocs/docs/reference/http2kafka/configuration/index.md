# HTTP2Kafka configuration { #configuration-http2kafka }

## Configuration

Data.Rentgen reads settings from `config.yml` in the current working directory.
Use `DATA_RENTGEN_CONFIG_FILE` to select another YAML file.

- [kafka][configuration-consumer-kafka]
- [producer-specific][configuration-producer-specific]
- [logging][configuration-http2kafka-logging]
- [monitoring][configuration-http2kafka-monitoring]
- [static_files][configuration-http2kafka-static-files]
- [openapi][configuration-http2kafka-openapi]
- [debug][configuration-http2kafka-debug]

::: data_rentgen.http2kafka.settings.Http2KafkaApplicationSettings
    options:
      show_root_heading: true

::: data_rentgen.server.settings.ServerSettings
    options:
      show_root_heading: true
