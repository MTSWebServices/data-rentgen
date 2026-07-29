from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio
from httpx2 import ASGITransport, AsyncClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi import FastAPI


@pytest_asyncio.fixture
async def test_client(test_server_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=test_server_app),
        base_url="http://data-rentgen",
    ) as result:
        yield result
