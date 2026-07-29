from http import HTTPStatus

import pytest
from httpx2 import AsyncClient

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_ping(test_client: AsyncClient):
    response = await test_client.get("/monitoring/ping")
    assert response.status_code == HTTPStatus.OK
    assert response.json() == {"status": "ok"}
