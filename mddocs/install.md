# Install Data.Rentgen { #overview-install }

## Requirements

- [Docker](https://docs.docker.com/engine/install/)
- [docker-compose](https://github.com/docker/compose/releases/)

## Install & run

Copy `docker-compose.yml` and `.env.docker` from this repo:

### "docker-compose.yml"

!include(../../docker-compose.yml)

### note ".env.docker"

!include(../../.env.docker)

Then start containers using `docker-compose`:

    ```console
    $ VERSION=latest docker compose --profile all up -d --wait
    ...
    ```

`docker-compose` will download required images, create containers and start them in a proper order. Options can be set via `.env.docker` file or `environment` section in `docker-compose.yml`.

`VERSION` is a tag of docker image. You can find all available tags [here](https://hub.docker.com/r/mtsrus/data-rentgen/tags).

### Access Data.Rentgen

After all containers are started and ready, you can:

- Browse frontend at <http://localhost:3000>
- Open REST API Swagger doc at <http://localhost:8000/docs>
