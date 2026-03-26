# HTTP2Kafka proxy { #http2kafka }

Some of OpenLineage integrations support only HttpTransport, but not KafkaTransport, e.g. Trino.

Data.Rentgen HTTP → Kafka proxy is optional component which provides a simple HTTP API receiving
[OpenLineage run events](https://openlineage.io/docs/spec/object-model) in JSON format and sending them to Kafka topic as is,
so they can be handled by {ref}`message-consumer` in a proper way.

## OpenLineage HttpTransport or KafkaTransport?

Introducing http2kafka into the chain reduces performance a bit:

- It parses all incoming events for validation and routing purposes. The larger the event, the slower the parsing.
- HTTP/HTTPS protocol is far more complex than Kafka TCP protocol, and has much higher latency in the first place.

If OpenLineage integration supports both HttpTransport and KafkaTransport, and Kafka doesn't use complex authentication not supported by OpenLineage (e.g. OAUTHBEARER), prefer KafkaTransport.

If this is not possible, http2kafka is the way to go.

## Install & run

### With docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile http2kafka up -d --wait
  ...
  ```

  `docker-compose` will download all necessary images, create containers, and then start the component.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

<!-- TODO везде, где literal include нужно сделать инклюды-->>

??? note "docker-compose.yml"

    ```yaml hl_lines="155-173" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="29-34" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

- After component is started and ready, open <http://localhost:8002/docs>.

### Without docker

- Install Python 3.10 or above

- Setup {ref}`message-broker`

- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[http2kafka]
  ...
  ```

- Run http2kafka process

  ```console
  $ python -m data_rentgen.http2kafka --host 0.0.0.0 --port 8002
  ...
  ```

  This is a thin wrapper around [uvicorn](https://www.uvicorn.org/#command-line-options) cli,
  options and commands are just the same.

- After server is started and ready, open [http://localhost:8002/docs](http://localhost:8002/docs).

## See also

[Configuration][configuration-http2kafka]
[OpenAPI][http2kafka-openapi]
[Alternatives][http2kafka-alternatives]
