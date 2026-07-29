from http import HTTPStatus

import pytest
from httpx2 import AsyncClient

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


async def test_ping(consumer_client: AsyncClient):
    response = await consumer_client.get("/monitoring/ping")
    assert response.status_code == HTTPStatus.NO_CONTENT
