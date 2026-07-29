from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import Tag
from tests.fixtures.mocks import MockedUser
from tests.test_server.utils.convert_to_json import tag_to_json
from tests.test_server.utils.enrich import enrich_tags

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_search_tags_by_name(
    test_client: AsyncClient,
    async_session: AsyncSession,
    tags_search: dict[str, Tag],
    mocked_user: MockedUser,
) -> None:
    tags = await enrich_tags([tags_search["company.product"]], async_session)

    response = await test_client.get(
        "/v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "product"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": tag.id,
                "data": tag_to_json(tag),
            }
            for tag in sorted(tags, key=lambda tag: tag.name.lower())
        ],
    }


async def test_search_tags_by_value(
    test_client: AsyncClient,
    async_session: AsyncSession,
    tags_search: dict[str, Tag],
    mocked_user: MockedUser,
) -> None:
    tags = await enrich_tags([tags_search["company.product"]], async_session)

    response = await test_client.get(
        "/v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "dataops"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 1,
        },
        "items": [
            {
                "id": tag.id,
                "data": tag_to_json(tag),
            }
            for tag in sorted(tags, key=lambda tag: tag.name.lower())
        ],
    }


async def test_search_tags_no_results(
    test_client: AsyncClient,
    tags_search: dict[str, Tag],
    mocked_user: MockedUser,
) -> None:
    response = await test_client.get(
        "/v1/tags",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"search_query": "not-found"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "page": 1,
            "page_size": 20,
            "pages_count": 1,
            "previous_page": None,
            "total_count": 0,
        },
        "items": [],
    }
