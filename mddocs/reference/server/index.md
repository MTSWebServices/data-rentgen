# REST API Server { #server }

Data.Rentgen REST API server provides simple HTTP API for accessing entities stored in [`database`][базе данных].
Implemented using [FastAPI](https://fastapi.tiangolo.com/).

## Install & run

### With docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile server up -d --wait
  ...
  ```

  `docker-compose` will download all necessary images, create containers, and then start the server.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

??? note "docker-compose.yml"

    ```yaml hl_lines="71-99" linenums="1"
    ----8<----
    docker-compose.yml
    ----8<----
    ```

??? note ".env.docker"

    ```ini hl_lines="22-27" linenums="1"
    ----8<----
    .env.docker
    ----8<----
    ```

- After server is started and ready, open [http://localhost:8000/docs](http://localhost:8000/docs).

### Without docker

- Install Python 3.10 or above

- Setup [`database`][database], run migrations and create partitions

- Create virtual environment

  ```console
  $ python -m venv /some/.venv
  ...
  $ source /some/.venv/activate
  ```

- Install `data-rentgen` package with following *extra* dependencies:

  ```console
  $ pip install data-rentgen[server,postgres]
  ...
  ```

- Run server process

  ```console
  $ python -m data_rentgen.server --host 0.0.0.0 --port 8000
  ...
  ```

  This is a thin wrapper around [uvicorn](https://www.uvicorn.org/#command-line-options) cli,
  options and commands are just the same.

- After server is started and ready, open [http://localhost:8000/docs](http://localhost:8000/docs).

## See also

- [Authentication and Authorization][auth-server]
- [REST API server configuration][configuration-server]
- [OpenAPI specification][server-openapi]
