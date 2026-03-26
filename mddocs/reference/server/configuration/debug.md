# Enabling debug { #configuration-server-debug }

## Return debug info in REST API responses

By default, server does not add error details to response bodies, to avoid exposing instance-specific information to end users.

You can change this by setting:

```console
$ export DATA_RENTGEN__SERVER__DEBUG=False
$ # start REST API server
$ curl -XPOST http://localhost:8000/failing/endpoint ...
{
    "error": {
        "code": "unknown",
        "message": "Got unhandled exception. Please contact support",
        "details": null,
    },
}
```

```console
$ export DATA_RENTGEN__SERVER__DEBUG=True
$ # start REST API server
$ curl -XPOST http://localhost:8000/failing/endpoint ...
Traceback (most recent call last):
File ".../uvicorn/protocols/http/h11_impl.py", line 408, in run_asgi
    result = await app(  # type: ignore[func-returns-value]
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File ".../site-packages/uvicorn/middleware/proxy_headers.py", line 84, in __call__
    return await self.app(scope, receive, send)
```

!!! warning

    This is only for development environment only. Do **NOT** use on production!

## Print debug logs on backend

See [Logging settings][configuration-server-logging], but replace log level `INFO` with `DEBUG`.

## Fill up `X-Request-ID` header on backend

Server can add `X-Request-ID` header to responses, which allows to match request on client with backend response.

This is done by `request_id` middleware, which is enabled by default and can configured as described below:

::: data_rentgen.server.settings.request_id.RequestIDSettings

## Print request ID to backend logs

This is done by adding a specific filter to logging handler:

??? note "logging.yml"

    ```yaml hl_lines="6-12 23-24 35"
    ----8<----
    data_rentgen/logging/presets/plain.yml
    ----8<----
    ```

Resulting logs look like:

```text
2023-12-18 17:14:11.711 uvicorn.access:498 [INFO] 018c15e97a068ae09484f8c25e2799dd 127.0.0.1:34884 - "GET /monitoring/ping HTTP/1.1" 200
```

## Use `X-Request-ID` header on client

If client got `X-Request-ID` header from backend, it is printed to logs with `DEBUG` level:

```pycon
>>> import logging
>>> logging.basicConfig(level=logging.DEBUG)
>>> client.ping()
DEBUG:urllib3.connectionpool:http://localhost:8000 "GET /monitoring/ping HTTP/1.1" 200 15
DEBUG:data_rentgen.client.base:Request ID: '018c15e97a068ae09484f8c25e2799dd'
```

Also, if REST API response was not successful, `Request ID` is added to exception message:

```pycon
>>> client.get_namespace("unknown")
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://localhost:8000/v1/namespaces/unknown
Request ID: '018c15eb80fa81a6b38c9eaa519cd322'
```

## Fill up `X-Application-Version` header on REST API side

Server can add `X-Application-Version` header to responses, which allows to determine which version of backend is deployed.

This is done by `application_version` middleware, which is enabled by default and can configured as described below:

::: data_rentgen.server.settings.application_version.ApplicationVersionSettings
