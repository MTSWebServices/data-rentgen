# syntax = docker/dockerfile:1.4

ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim AS base

WORKDIR /app
ENV PYTHONPATH=/app \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # required for GSSAPI Kafka auth
        krb5-user \
        # Used for healthcheck
        curl \
    && rm -rf /var/lib/apt/lists/* /var/cache/*

# Logos
COPY --chmod=755 ./docs/_static/*.svg ./data_rentgen/server/static/

# Swagger UI
ADD --chmod=755 https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui-bundle.js https://cdn.jsdelivr.net/npm/swagger-ui-dist@latest/swagger-ui.css \
    /app/data_rentgen/server/static/swagger/

# Redoc
ADD --chmod=755 https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js /app/data_rentgen/server/static/redoc/redoc.standalone.js

ENV DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__JS_URL=/static/swagger/swagger-ui-bundle.js \
    DATA_RENTGEN__SERVER__OPENAPI__SWAGGER__CSS_URL=/static/swagger/swagger-ui.css \
    DATA_RENTGEN__SERVER__OPENAPI__REDOC__JS_URL=/static/redoc/redoc.standalone.js \
    DATA_RENTGEN__SERVER__STATIC_FILES__DIRECTORY=/app/data_rentgen/server/static


FROM base AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        gcc \
        make \
        # required to build gssapi from sources
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/* /var/cache/*


RUN --mount=type=cache,target=/root/.cache/pip \
    pip install uv

COPY ./pyproject.toml ./uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --frozen \
        --link-mode copy \
        --no-install-project \
        --all-extras \
        --compile-bytecode \
    && uv pip install pip \
    && uv cache prune --ci

FROM base AS prod

# We don't need uv and compilers in final image
COPY --from=builder /app/.venv/ /app/.venv/
COPY --chmod=755 ./data_rentgen/ /app/data_rentgen/
RUN python -m compileall data_rentgen

# Do not run production as root, to improve security.
# Also user does not own anything inside the image, including venv and source code.
RUN useradd data-rentgen
USER data-rentgen
