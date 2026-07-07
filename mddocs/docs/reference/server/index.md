# REST API Server { #server }

Data.Rentgen REST API server provides simple HTTP API for accessing entities stored in [`database`][database].
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

    ```yaml title="docker-compose.yml"
    --8<--
    docker-compose.yml:71:99
    --8<--
    ```

    ```ini title=".env.docker"
    --8<--
    .env.docker:22:27
    --8<--
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

## Advanced usage

Instead of relying on environment variables, application can also be configured by explicitly building it in Python code:

```python
from data_rentgen.server import application_factory, ServerApplicationSettings

app = application_factory(ServerApplicationSettings(...))
```

## See also

- [Authentication and Authorization][auth-server]
- [REST API server configuration][configuration-server]
- [OpenAPI specification][server-openapi]
