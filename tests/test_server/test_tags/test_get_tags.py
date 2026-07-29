from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Tag
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import tag_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_tags_no_filters(
    test_client: AsyncClient,
    tags: list[Tag],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": len(tags),
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            {
                "id": tag.id,
                "data": tag_to_json(tag),
            }
            for tag in sorted(tags, key=lambda tag: tag.name.lower())
        ],
    }


async def test_get_tags_unauthorized(test_client: AsyncClient):
    response = await test_client.get("v1/tags")
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_tags_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
