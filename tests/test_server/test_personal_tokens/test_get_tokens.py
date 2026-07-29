from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http import HTTPStatus

import pytest
from dirty_equals import IsDate
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.personal_token import create_personal_token
from tests.test_server.fixtures.factories.user import create_user
from tests.test_server.utils.convert_to_json import personal_token_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_get_personal_tokens(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()
    valid_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={"name": "abc"},
    )
    expired_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "cde",
            "since": today - timedelta(days=10),
            "until": today - timedelta(days=1),
        },
    )
    _revoked_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={"revoked_at": datetime.now(tz=UTC)},
    )

    another_user = await create_user(async_session)
    _foreign_token = await create_personal_token(async_session, another_user)

    current_token_claims = personal_token_jwt_decoder(mocked_user.personal_token)

    response = await test_client.get(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        "meta": {
            "page": 1,
            "page_size": 20,
            "total_count": 3,
            "pages_count": 1,
            "has_next": False,
            "has_previous": False,
            "next_page": None,
            "previous_page": None,
        },
        "items": [
            # Include only non-revoked tokens of current user, plus current token
            {
                "id": str(valid_token.id),
                "data": personal_token_to_json(valid_token),
            },
            {
                "id": str(expired_token.id),
                "data": personal_token_to_json(expired_token),
            },
            {
                "id": current_token_claims["jti"],
                "data": {
                    "id": current_token_claims["jti"],
                    "name": current_token_claims["token_name"],
                    "scopes": current_token_claims["scope"].split(" "),
                    "since": IsDate(iso_string=True),
                    "until": IsDate(iso_string=True),
                },
            },
        ],
    }


async def test_get_personal_tokens_only_last_token_with_name_is_returned(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()
    valid_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "abc",
            "since": today,
        },
    )
    _expired_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "abc",
            "since": today - timedelta(days=10),
            "until": today - timedelta(days=1),
        },
    )

    current_token_claims = personal_token_jwt_decoder(mocked_user.personal_token)

    response = await test_client.get(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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
                "id": str(valid_token.id),
                "data": personal_token_to_json(valid_token),
            },
            # expired token with same name is not returned
            {
                "id": current_token_claims["jti"],
                "data": {
                    "id": current_token_claims["jti"],
                    "name": current_token_claims["token_name"],
                    "scopes": current_token_claims["scope"].split(" "),
                    "since": IsDate(iso_string=True),
                    "until": IsDate(iso_string=True),
                },
            },
        ],
    }


async def test_get_personal_tokens_unauthorized(test_client: AsyncClient):
    response = await test_client.get("v1/personal-tokens")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_get_personal_tokens_via_personal_token_is_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
