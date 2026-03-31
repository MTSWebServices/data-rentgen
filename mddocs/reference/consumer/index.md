# Message Consumer { #message-consumer }

Data.Rentgen fetches messages from a [`message-broker`][message-broker] using a [FastStream](https://faststream.airt.ai) based consumer, parses incoming messages, and creates all parsed entities in the [`database`][database]. Malformed messages are send back to broker, to different topic.

## Install & run

### With docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile consumer up -d --wait
  ...
  ```

  `docker-compose` will download all necessary images, create containers, and then start consumer process.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

  ??? note "docker-compose.yml"
      --8<--
      docker-compose.yml:120:138
      --8<--

  ??? note ".env.docker"
      --8<--
      .env.docker:22:24,29:34
      --8<--

### Without docker

- Install Python 3.10 or above
- Setup [`database`][database], run migrations and create partitions
- Setup [`message-broker`][message-broker]
- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ...
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[consumer,postgres]
  ...
  ```

  !!! note
      For `SASL_GSSAPI` auth mechanism you also need to install system packages providing `kinit` and `kdestroy` binaries:
      ```console
      $ apt install libkrb5-dev krb5-user gcc make autoconf  # Debian-based
      ...
      $ dnf install krb5-devel krb5-libs krb5-workstation gcc make autoconf  # CentOS, OracleLinux
      ...
      ```
      And then install `gssapi` extra:
      ```console
      $ pip install data-rentgen[consumer,postgres,gssapi]
      ...
      ```

- Run consumer process

  ```console
  $ python -m data_rentgen.consumer
  ...
  ```

## See also

[Consumer configuration][configuration-consumer]
