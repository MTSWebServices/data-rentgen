from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio
from httpx2 import ASGITransport, AsyncClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from faststream.asgi import AsgiFastStream


@pytest_asyncio.fixture
async def consumer_client(test_consumer_app: AsgiFastStream) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=test_consumer_app),
        base_url="http://data-rentgen",
    ) as result:
        yield result
