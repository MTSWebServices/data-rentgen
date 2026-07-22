# Consumer configuration { #configuration-consumer }

Data.Rentgen reads settings from `config.yml` in the current working directory.
Use `DATA_RENTGEN_CONFIG_FILE` to select another YAML file.

::: data_rentgen.consumer.settings.ConsumerApplicationSettings
    options:
        docstring_style: sphinx
        members:
            - database
            - logging
            - kafka
            - consumer
            - producer
            - monitoring
