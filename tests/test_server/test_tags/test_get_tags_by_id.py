from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Tag
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import tag_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_tags_by_unknown_id(
    test_client: AsyncClient,
    new_tag: Tag,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_id": new_tag.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 0,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [],
    }


async def test_get_tags_by_one_id(
    test_client: AsyncClient,
    tags: Tag,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    tag = tags[0]
    response = await test_client.get(
        "v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_id": tag.id},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 1,
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
            },
        ],
    }


async def test_get_tags_by_multiple_ids(
    test_client: AsyncClient,
    tags: list[Tag],
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    selected_tags = tags[:2]
    response = await test_client.get(
        "v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"tag_id": [tag.id for tag in selected_tags]},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 2,
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
            for tag in sorted(selected_tags, key=lambda tag: tag.name.lower())
        ],
    }
