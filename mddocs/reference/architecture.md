# Architecture { #Architecture }

## Components

Data.Rentgen is build using following components:

- [OpenLineage](https://openlineage.io/docs/) clients & integrations with third-party modules (e.g. Apache Spark, Apache Airflow).
- [`message-broker`][message-broker], receiving events in JSON format.
- [`message-consumer`][message-consumer], parsing JSON messages.
- [`database`][database] for storing consumed & cleaned up data.
- [`server`][server], serving database data.
- [`frontend`][frontend], accessing REST API to navigate created entities & lineage graph.
- [`http2kafka`][http2kafka] (optional), proxy for sending OpenLineage events to Kafka using HTTP API.

## Architecture diagram

```mermaid
stateDiagram-v2
direction LR

state "OpenLineage" as OpenLineage {
state "OpenLineage Spark" as SPARK
state "OpenLineage Airflow" as AIRFLOW
state "OpenLineage Hive" as HIVE
state "OpenLineage Flink" as FLINK
state "OpenLineage dbt" as DBT
state "OpenLineage other" as OTHER
state "OpenLineage KafkaTransport" as KAFKA_TRANSPORT
state "OpenLineage HttpTransport" as HTTP_TRANSPORT
SPARK --> KAFKA_TRANSPORT
AIRFLOW --> KAFKA_TRANSPORT
HIVE --> KAFKA_TRANSPORT
FLINK --> KAFKA_TRANSPORT
DBT --> KAFKA_TRANSPORT
KAFKA_TRANSPORT --> KAFKA
OTHER --> HTTP_TRANSPORT
HTTP_TRANSPORT --> HTTP2KAFKA
}
state User
USER --> FRONTEND
state "Data.Rentgen" as Data.Rentgen {
state "Kafka"  as KAFKA
state "Message consumer" as CONSUMER
state "PostgreSQL" as DB
state "REST API server" as API
state "Frontend" as FRONTEND
state "HTTP2Kafka" as HTTP2KAFKA

HTTP2KAFKA --> KAFKA

KAFKA --> CONSUMER
CONSUMER --> DB

API --> DB
FRONTEND --> API
}
```
