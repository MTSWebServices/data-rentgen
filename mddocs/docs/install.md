# Install Data.Rentgen { #overview-install }

## Requirements

- [Docker](https://docs.docker.com/engine/install/)
- [docker-compose](https://github.com/docker/compose/releases/)

## Install & run

Copy `docker-compose.yml` and `config.docker.yml` from this repo:

??? note "docker-compose.yml"
    ```yaml
    --8<-- "docker-compose.yml"
    ```

??? note "config.docker.yml"
    ```yaml
    --8<-- "config.docker.yml"
    ```

Then start containers using `docker-compose`:

```console
$ VERSION=latest docker compose --profile all up -d --wait
...
```

`docker-compose` will download required images, create containers and start them in a proper order.
Settings are loaded from `config.docker.yml`.

`VERSION` is a tag of docker image. You can find all available tags [here](https://hub.docker.com/r/mtsrus/data-rentgen/tags).

### Access Data.Rentgen

After all containers are started and ready, you can:

- Browse frontend at <http://localhost:3000>
- Open REST API Swagger doc at <http://localhost:8000/docs>
