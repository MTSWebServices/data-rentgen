from collections.abc import Callable
from datetime import UTC, datetime
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import PersonalToken, User
from data_rentgen.server.settings import ServerApplicationSettings as Settings
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.personal_token import create_personal_token

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_personal_token_auth(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"name": mocked_user.user.name}


@pytest.mark.parametrize(
    "server_app_settings",
    [
        {"auth": {"personal_tokens": {"enabled": False}}},
    ],
    indirect=True,
)
async def test_personal_token_auth_disabled(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    server_app_settings: Settings,
):
    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Authentication using PersonalTokens is disabled",
            "details": None,
        },
    }


async def test_personal_token_auth_invalid_token(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/datasets",
        headers={"Authorization": f"Bearer {mocked_user.personal_token + 'invalid'}"},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "Signature verification failed",
        },
    }


async def test_personal_token_auth_expired_token(
    test_client: AsyncClient,
    user: User,
    personal_token: PersonalToken,
    personal_token_jwt_generator: Callable[..., str],
):
    token = personal_token_jwt_generator(personal_token, user, time_delta=-1000)

    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "Token has expired",
        },
    }


async def test_personal_token_auth_revoked_token(
    async_session: AsyncSession,
    test_client: AsyncClient,
    user: User,
    personal_token: PersonalToken,
    personal_token_jwt_generator: Callable[..., str],
):
    personal_token = await create_personal_token(
        async_session,
        user,
        token_kwargs={
            "revoked_at": datetime.now(tz=UTC).date(),
        },
    )
    token = personal_token_jwt_generator(personal_token, user)
    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": f"PersonalToken name='{personal_token.name}', id={personal_token.id} is revoked",
        },
    }
