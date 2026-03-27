# Message Broker { #message-broker }

Message broker is component used by OpenLineage to store all received events. Then these avents are handled by [`message-consumer`][message-consumer], in batches.

Currently, Data.Rentgen supports only [Apache Kafka](https://kafka.apache.org/) as message broker.

## Why Kafka?

Other popular OpenLineage server implementations use HTTP protocol for receiving events. In out experience, Kafka is much superior for this case:

- Kafka is designed to be scalable. If performance level is not enough, just add another broker to the cluster. For HTTP servers it's not that simple,
  as this requires load balancing on reverse proxy side or DNS side.
- Kafka is designed to receive A LOT of events per second, like millions, and store them on disk as fast as possible. So no events are lost
  even if [`message-consumer`][message-consumer] is overloaded - events are already on disk, and will be handled later.
- ETL scripts are mostly run on schedule The usual pattern is almost zero events during the day, but huge spikes at every whole hour
  (e.g. at 00:00, 01:00, 03:00, 12:00). Kafka is used as an intermediate buffer which smooths these spikes.
- Events stored in Kafka can be read in batches, even if OpenLineage integration initially send them one-by-one.
  Batching gives x10 more performance than handling individual events.
- HTTP/HTTPS protocol have higher latency than Kafka TCP protocol. Some OpenLineage integrations are sensitive to latency - for example,
  [Flink job listener documentation](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/core/execution/JobListener.html)
  explicitly says: *If you block the thread the invoker of environment execute methods is possibly blocked*. The less time required for sending response, the better.

## Requirements

- Apache Kafka 3.x. It is recommended to use latest Kafka version.

### Setup

#### With Docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile broker up -d --wait
  ...
  ```

  `docker-compose` will download Apache Kafka image, create container and volume, and then start container.

  Image entrypoint will create database if volume is empty.
  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

!!! note "docker-compose.yml"
    --8<--
    docker-compose.yml:101:117, 177
    --8<--

!!! note ".env.docker"
    --8<--
    .env.docker:7:20
    --8<--

#### Without Docker

Please follow [Apache Kafka installation instruction](https://kafka.apache.org/quickstart#quickstart_startserver).
