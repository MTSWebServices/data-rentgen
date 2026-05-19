#!make

include .env.local

VERSION = develop
VIRTUAL_ENV ?= .venv
PYTHON = ${VIRTUAL_ENV}/bin/python
PIP = ${VIRTUAL_ENV}/bin/pip
UV ?= ${VIRTUAL_ENV}/bin/uv
PYTEST = ${VIRTUAL_ENV}/bin/pytest
COVERAGE = ${VIRTUAL_ENV}/bin/coverage
GITHUB_RUN_ID ?= unknown

# Fix docker build and docker compose build using different backends
COMPOSE_DOCKER_CLI_BUILD = 1
DOCKER_BUILDKIT = 1
# Fix docker build on M1/M2
DOCKER_DEFAULT_PLATFORM = linux/amd64

HELP_FUN = \
	%help; while(<>){push@{$$help{$$2//'options'}},[$$1,$$3] \
	if/^([\w-_]+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/}; \
    print"$$_:\n", map"  $$_->[0]".(" "x(20-length($$_->[0])))."$$_->[1]\n",\
    @{$$help{$$_}},"\n" for keys %help; \

all: help

help: ##@Help Show this help
	@echo -e "Usage: make [target] ...\n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)



venv: venv-cleanup venv-install##@Env Init venv and install uv dependencies

venv-cleanup: ##@Env Cleanup venv
	@rm -rf ${VIRTUAL_ENV} || true
	python3 -m venv ${VIRTUAL_ENV}
	${PIP} install uv

venv-install: ##@Env Install requirements to venv
	${UV} sync --inexact --no-install-project --frozen --all-extras --all-groups --no-extra gssapi $(ARGS)
	${UV} pip install --no-deps sphinx-plantuml


db: db-start db-upgrade db-partitions ##@DB Prepare database (in docker)

db-start: ##@DB Start database
	docker compose -f docker-compose.yml up -d --wait db $(DOCKER_COMPOSE_ARGS)

db-revision: ##@DB Generate migration file
	${PYTHON} -m data_rentgen.db.migrations revision --autogenerate $(ARGS)

db-upgrade: ##@DB Run migrations to head
	${PYTHON} -m data_rentgen.db.migrations upgrade head $(ARGS)

db-downgrade: ##@DB Downgrade head migration
	${PYTHON} -m data_rentgen.db.migrations downgrade head-1 $(ARGS)

db-partitions: ##@DB Create partitions
	${PYTHON} -m data_rentgen.db.scripts.create_partitions --start 2024-07-01

db-cleanup-partitions: ##@DB Clean partitions
	${PYTHON} -m data_rentgen.db.scripts.cleanup_partitions $(ARGS)

db-cleanup-partitions-ci: ##@DB Clean partitions in CI
	${COVERAGE} run -m data_rentgen.db.scripts.cleanup_partitions $(ARGS)

db-views: ##@DB Create views
	${UV} run coverage run python -m data_rentgen.db.scripts.refresh_analytic_views $(ARGS)

db-seed: ##@DB Seed database with random data
	${PYTHON} -m data_rentgen.db.scripts.seed $(ARGS)

db-seed-ci: ##@DB Seed database with random data for CI
	${COVERAGE} run -m data_rentgen.db.scripts.seed $(ARGS)

broker: broker-start ##@Broker Prepare broker (in docker)

broker-start: ##Broker Start broker
	docker compose -f docker-compose.yml up -d --wait broker $(DOCKER_COMPOSE_ARGS)


test: test-db test-broker ##@Test Run tests
	${PYTEST} $(PYTEST_ARGS)

test-lineage: test-db ##@Test Run linege tests
	${PYTEST} -m lineage $(PYTEST_ARGS)

test-server: test-db ##@Test Run server tests
	${PYTEST} -m server $(PYTEST_ARGS)

test-db: test-db-start db-upgrade db-partitions ##@TestDB Prepare database (in docker)

test-db-start: ##@TestDB Start database
	docker compose -f docker-compose.test.yml up -d --wait db $(DOCKER_COMPOSE_ARGS)

test-broker: test-broker-start ##@TestBroker Prepare broker (in docker)

test-broker-start: ##@TestBroker Start broker
	docker compose -f docker-compose.test.yml up -d --wait broker $(DOCKER_COMPOSE_ARGS)

test-ci: test-db test-broker ##@Test Run CI tests
	${COVERAGE} run -m pytest --junitxml=reports/junit/${GITHUB_RUN_ID}.xml $(PYTEST_ARGS)

test-check-fixtures: ##@Test Check declared fixtures
	${PYTEST} --dead-fixtures $(PYTEST_ARGS)

test-cleanup: ##@Test Cleanup tests dependencies
	docker compose -f docker-compose.test.yml --profile all down --remove-orphans $(ARGS)



dev-server: db-start ##@Application Run development server (without docker)
	${PYTHON} -m data_rentgen.server --host 0.0.0.0 --port 8000 $(ARGS)

dev-consumer: db-start broker-start ##@Application Run development broker (without docker)
	${PYTHON} -m data_rentgen.consumer --host 0.0.0.0 --port 8001 $(ARGS)

dev-http2kafka: broker-start ##@Application Run development http2kafka (without docker)
	${PYTHON} -m data_rentgen.http2kafka --host 0.0.0.0 --port 8002 $(ARGS)

prod-build: ##@Application Build docker image
	docker build --progress=plain --network=host -t mtsrus/data-rentgen:develop -f ./docker/Dockerfile $(ARGS) .

prod: ##@Application Run production containers
	docker compose -f docker-compose.yml --profile all up -d $(ARGS)

prod-cleanup: ##@Application Stop production containers
	docker compose -f docker-compose.yml --profile all down --remove-orphans $(ARGS)


.PHONY: docs

docs: docs-build docs-open ##@Docs Generate & open docs

docs-build: ##@Docs Generate docs
	$(MAKE) -C docs html

docs-open: ##@Docs Open docs
	xdg-open docs/_build/html/index.html

docs-cleanup: ##@Docs Cleanup docs
	$(MAKE) -C docs clean

docs-fresh: docs-cleanup docs-build ##@Docs Cleanup & build docs

docs-openapi: ##@Docs Generate OpenAPI schema
	${PYTHON} -m data_rentgen.server.scripts.export_openapi_schema docs/_static/openapi_server.json
	${PYTHON} -m data_rentgen.http2kafka.scripts.export_openapi_schema docs/_static/openapi_http2kafka.json
