# Frontend { #frontend }

Data.Rentgen provides a [Frontend (UI)](https://github.com/MobileTeleSystems/data-rentgen-ui) based on [ReactAdmin](https://marmelab.com/react-admin/) and [ReactFlow](https://reactflow.dev/),
providing users the ability to navigate entities and build lineage graph.

## Install & run

### With Docker

- Install [Docker](https://docs.docker.com/engine/install/)

- Install [docker-compose](https://github.com/docker/compose/releases/)

- Run the following command:

  ```console
  $ docker compose --profile frontend up -d --wait
  ...
  ```

  `docker-compose` will download Data.Rentgen UI image, create containers, and then start them.

  Options can be set via `.env` file or `environment` section in `docker-compose.yml`

??? note "docker-compose.yml"

  ```yaml hl_lines="140-151" linenums="1"
  ----8<----
  docker-compose.yml
  ----8<----
  ```  

??? note ".env.docker"

  ```ini hl_lines="36-37" linenums="1"
  ----8<----
  .env.docker
  ----8<----
  ```

- After frontend is started and ready, open <http://localhost:3000>.

## See also

[Configuration][configuration-frontend]
