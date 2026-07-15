from http import HTTPStatus

import pytest
from httpx import AsyncClient

pytestmark = [pytest.mark.consumer, pytest.mark.asyncio]


async def test_metrics(consumer_client: AsyncClient):
    response = await consumer_client.get("/monitoring/metrics")
    assert response.status_code == HTTPStatus.OK
    assert "faststream_received_processed_messages_total" in response.text
    assert "faststream_received_processed_messages_exceptions_total" in response.text
