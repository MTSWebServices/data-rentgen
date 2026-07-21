# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import FastAPI
from starlette.datastructures import MutableHeaders

from data_rentgen.server.settings.application_version import ApplicationVersionSettings

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send


@dataclass(slots=True)
class ApplicationVersionMiddleware:
    app: ASGIApp
    version: str
    header_name: str = "X-Application-Version"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in {"http", "websocket"}:
            # lifespan or other type of request
            await self.app(scope, receive, send)
            return

        async def modify_response_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append(self.header_name, self.version)

            await send(message)

        await self.app(scope, receive, modify_response_headers)


def apply_application_version_middleware(app: FastAPI, settings: ApplicationVersionSettings) -> FastAPI:
    """Add X-Application-Version middleware to the application."""
    if not settings.enabled:
        return app

    app.add_middleware(
        ApplicationVersionMiddleware,
        version=app.version,
        **settings.model_dump(exclude={"enabled"}),
    )
    return app
