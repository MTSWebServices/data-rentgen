# Alternatives { #http2kafka-alternatives }

HTTP → Kafka proxy is build with mandatory authentication using personal tokens. This may be a drawback for some use cases.

If your use case requires sending OpenLineage events to DataRentgen via HTTP, but without any authentication, there are some alternatives described below.

## Fluentbit

[Fluentbit](https://fluentbit.io/) is a lightweight yet powerful logging processor written on C.
It can accept [HTTP requests](https://docs.fluentbit.io/manual/data-pipeline/inputs/http) and write body to [Kafka topic](https://docs.fluentbit.io/manual/data-pipeline/outputs/kafka).

Config example:

```yaml title="fluent-bit.yml"

pipeline:
    # receive HTTP requests on port 8002
    inputs:
      - name: http
        port: 8002
        mem_buf_limit: 50MB

    # Route events to partition using message key
    filters:
      - name: lua
        match: '*'
        call: set_message_key
        code:  |
            function set_message_key(tag, timestamp, record)
            local new_record = record
            if record.run.facets.parent then
                if record.run.facets.parent.root then
                new_record.messageKey = "run:" .. record.run.facets.parent.root.job.namespace .. "/" .. record.run.facets.parent.root.job.name
                else
                new_record.messageKey = "run:" .. record.run.facets.parent.job.namespace .. "/" .. record.run.facets.parent.job.name
                end
            else
                new_record.messageKey = "run:" .. record.job.namespace .. "/" .. record.job.name
            end
            return 1, timestamp, new_record
            end

    # Write data to Kafka topic
    outputs:
      - name: kafka
        match: '*'
        format: json
        timestamp_key: eventTime
        timestamp_format: iso8601_ns
        message_key_field: messageKey
        brokers: localhost:9093
        topics: input.runs
        rdkafka.security.protocol: SASL_PLAINTEXT
        rdkafka.sasl.mechanism: SCRAM-SHA-256
        rdkafka.sasl.username: data_rentgen
        rdkafka.sasl.password: changeme
        rdkafka.client.id: fluent-bit
        rdkafka.request.required.acks: 1
        rdkafka.log.connection.close: false
```

```yaml title="docker-compose.yml"

services:
    fluent-bit:
        image: central-mirror.services.mts.ru/fluent/fluent-bit
        restart: unless-stopped
        command: --config /fluent-bit/etc/fluent-bit.yml
        volumes:
        - ./fluent-bit.yml:/fluent-bit/etc/fluent-bit.yml
        # Kafka hostnames should be resolvable from container network
        network_mode: host
```
