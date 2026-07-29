from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models.personal_token import PersonalToken
from data_rentgen.server.settings import ServerApplicationSettings as Settings
from data_rentgen.utils.uuid import extract_timestamp_from_uuid
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.personal_token import create_personal_token
from tests.test_server.utils.convert_to_json import personal_token_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


def day_beginning(inp: date) -> datetime:
    return datetime.combine(inp, datetime.min.time(), tzinfo=UTC)


async def test_create_personal_token(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()
    until = today + timedelta(days=366)

    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": "test"},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    query = select(PersonalToken).where(PersonalToken.user_id == mocked_user.user.id, PersonalToken.name == "test")
    personal_token = await async_session.scalar(query)

    assert personal_token is not None
    assert personal_token.user_id == mocked_user.user.id
    assert personal_token.since == today
    assert personal_token.until == until
    assert personal_token.revoked_at is None

    response_data = response.json()
    assert response_data.keys() == {"id", "data", "content"}
    assert response_data["id"] == str(personal_token.id)
    assert response_data["data"] == personal_token_to_json(personal_token)

    jwt = response_data["content"]
    assert jwt
    assert personal_token_jwt_decoder(jwt) == {
        "jti": str(personal_token.id),
        "iss": "data-rentgen",
        "token_name": "test",
        "sub_id": mocked_user.user.id,
        "preferred_username": mocked_user.user.name,
        "scope": "all:read all:write",
        "iat": int(extract_timestamp_from_uuid(personal_token.id).timestamp()),
        "nbf": day_beginning(personal_token.since).timestamp(),
        "exp": day_beginning(personal_token.until + timedelta(days=1)).timestamp(),
    }


async def test_create_personal_token_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()
    until = today + timedelta(days=1)

    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": "test", "until": until.isoformat()},
    )

    assert response.status_code == HTTPStatus.OK, response.json()

    query = select(PersonalToken).where(PersonalToken.user_id == mocked_user.user.id, PersonalToken.name == "test")
    personal_token = await async_session.scalar(query)

    assert personal_token is not None
    assert personal_token.user_id == mocked_user.user.id
    assert personal_token.since == today
    assert personal_token.until == until
    assert personal_token.revoked_at is None

    response_data = response.json()
    assert response_data.keys() == {"id", "data", "content"}
    assert response_data["id"] == str(personal_token.id)
    assert response_data["data"] == personal_token_to_json(personal_token)

    jwt = response_data["content"]
    assert jwt
    assert personal_token_jwt_decoder(jwt) == {
        "jti": str(personal_token.id),
        "iss": "data-rentgen",
        "token_name": "test",
        "sub_id": mocked_user.user.id,
        "preferred_username": mocked_user.user.name,
        "scope": "all:read all:write",
        "iat": int(extract_timestamp_from_uuid(personal_token.id).timestamp()),
        "nbf": day_beginning(personal_token.since).timestamp(),
        "exp": day_beginning(personal_token.until + timedelta(days=1)).timestamp(),
    }


async def test_create_personal_token_with_until_more_than_max_value(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    until = today + timedelta(days=1000)
    max_until = today + timedelta(days=366)

    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": "test", "until": until.isoformat()},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["body", "until"],
                    "code": "less_than_equal",
                    "message": f"Input should be less than or equal to {max_until.isoformat()}",
                    "context": {"le": max_until.isoformat()},
                    "input": until.isoformat(),
                },
            ],
        },
    }


async def test_create_personal_token_with_until_in_past(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    until = today - timedelta(days=1)

    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": "test", "until": until.isoformat()},
    )

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "Invalid request",
            "details": [
                {
                    "location": ["body", "until"],
                    "code": "date_future",
                    "message": "Date should be in the future",
                    "context": {},
                    "input": until.isoformat(),
                },
            ],
        },
    }


async def test_create_personal_token_already_exists(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    token = await create_personal_token(async_session, mocked_user.user)

    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": token.name},
    )

    assert response.status_code == HTTPStatus.CONFLICT, response.json()
    assert response.json() == {
        "error": {
            "code": "conflict",
            "message": f"PersonalToken with name={token.name!r} already exists",
            "details": {
                "entity_type": "PersonalToken",
                "field": "name",
                "value": token.name,
            },
        },
    }


async def test_create_personal_token_unauthorized(
    test_client: AsyncClient,
):
    response = await test_client.post("v1/personal-tokens", json={"name": "test"})

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


@pytest.mark.parametrize(
    "server_app_settings",
    [
        {"auth": {"personal_tokens": {"enabled": False}}},
    ],
    indirect=True,
)
async def test_create_personal_token_disabled(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    server_app_settings: Settings,
):
    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"name": "test"},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN, response.json()
    assert response.json() == {
        "error": {
            "code": "forbidden",
            "message": "Action not allowed",
            "details": "Authentication using PersonalTokens is disabled",
        },
    }


async def test_create_personal_token_via_personal_token_not_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
):
    response = await test_client.post(
        "v1/personal-tokens",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        json={"name": "test"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "PersonalToken was passed but access token was expected",
        },
    }
