import time
from collections.abc import Callable
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models import User
from data_rentgen.server.settings.auth.jwt import JWTSettings
from tests.fixtures.mocks import MockedUser

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


async def test_dummy_token_auth(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {"name": mocked_user.user.name}


async def test_dummy_auth_invalid_token(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/users/me",
        headers={"Authorization": f"Bearer {mocked_user.access_token + 'invalid'}"},
    )
    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "Signature verification failed",
        },
    }


async def test_dummy_auth_expired_token(
    test_client: AsyncClient,
    user: User,
    access_token_generator: Callable[..., str],
):
    token = access_token_generator(user, time_delta=-1000)

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


async def test_dummy_auth_logout_not_implemented(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.get(
        "v1/auth/logout",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )

    assert response.status_code == HTTPStatus.NOT_IMPLEMENTED, response.json()
    assert response.json() == {
        "error": {
            "code": "not_implemented",
            "message": "Logout method is not supported by DummyAuthProvider",
            "details": None,
        },
    }


async def test_dummy_auth_login(
    test_client: AsyncClient,
    async_session: AsyncSession,
    access_token_jwt_decoder: Callable[[str], dict],
    access_token_settings: JWTSettings,
):
    before = time.time()
    response = await test_client.post("v1/auth/token", data={"username": "test", "password": "test"})
    after = time.time()

    assert response.status_code == HTTPStatus.OK, response.json()

    data = response.json()
    assert data["token_type"] == "bearer"
    assert data["access_token"]
    assert before <= data["expires_at"] <= after + access_token_settings.expire_seconds

    claims = access_token_jwt_decoder(data["access_token"])
    query = select(User).where(User.name == "test").limit(1)
    user = await async_session.scalar(query)
    assert user

    assert claims["iss"] == "data-rentgen"
    assert claims["preferred_username"] == user.name
    assert claims["sub_id"] == user.id
    assert before <= claims["nbf"] <= after
    assert before <= claims["iat"] <= after
    assert claims["exp"] == data["expires_at"]
    assert "jti" not in claims
