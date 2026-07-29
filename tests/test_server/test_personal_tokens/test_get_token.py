from datetime import UTC, datetime, timedelta
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.personal_token import create_personal_token, personal_token_factory
from tests.test_server.fixtures.factories.user import create_user
from tests.test_server.utils.convert_to_json import personal_token_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_personal_token(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    valid_token = await create_personal_token(async_session, mocked_user.user)
    _another_valid_token = await create_personal_token(async_session, mocked_user.user)
    expired_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "since": today - timedelta(days=10),
            "until": today - timedelta(days=1),
        },
    )
    _another_expired_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "since": today - timedelta(days=10),
            "until": today - timedelta(days=1),
        },
    )

    another_user = await create_user(async_session)
    _foreign_token = await create_personal_token(async_session, another_user)

    expected_tokens = [valid_token, expired_token]

    response = await test_client.get(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        params={"personal_token_id": [str(valid_token.id), str(expired_token.id)]},
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
                "id": str(token.id),
                "data": personal_token_to_json(token),
            }
            for token in sorted(expected_tokens, key=lambda t: (t.name, -t.since.toordinal()))
        ],
    }


async def test_get_personal_token_not_found(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    revoked_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={"revoked_at": datetime.now(tz=UTC)},
    )

    another_user = await create_user(async_session)
    foreign_token = await create_personal_token(async_session, another_user)

    fake_token = personal_token_factory()

    for token in [revoked_token, foreign_token, fake_token]:
        response = await test_client.get(
            "v1/personal-tokens",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            params={"personal_token_id": str(token.id)},
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
