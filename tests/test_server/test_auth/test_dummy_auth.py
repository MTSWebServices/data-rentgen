from collections.abc import Callable
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient

from data_rentgen.db.models import User
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
            "message": "Logout method is not implemented for DummyAuthProvider",
            "details": None,
        },
    }


async def test_dummy_auth_login(
    test_client: AsyncClient,
    access_token_jwt_decoder: Callable[[str], dict],
):
    response = await test_client.post("v1/auth/token", data={"username": "test", "password": "test"})

    data = response.json()
    assert data["token_type"] == "bearer"
    assert data["access_token"]
    assert data["expires_at"]

    claims = access_token_jwt_decoder(data["access_token"])
    assert claims["iss"]
    assert claims["preferred_username"] == "test"
    assert claims["sub_id"]
    assert claims["nbf"]
    assert claims["iat"]
    assert claims["exp"]
    assert "jti" not in claims
