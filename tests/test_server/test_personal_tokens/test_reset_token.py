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
from tests.test_server.fixtures.factories.personal_token import (
    create_personal_token,
    personal_token_factory,
)
from tests.test_server.fixtures.factories.user import create_user
from tests.test_server.utils.convert_to_json import personal_token_to_json

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


def day_beginning(inp: date) -> datetime:
    return datetime.combine(inp, datetime.min.time(), tzinfo=UTC)


# user can take rxpired token and create a copy with new duration
@pytest.mark.parametrize(
    ["since_delta", "until_delta"],
    [
        (timedelta(days=-10), timedelta(days=-5)),
        (timedelta(days=-10), timedelta(days=0)),
        (timedelta(days=-10), timedelta(days=5)),
        (timedelta(days=0), timedelta(days=0)),
        (timedelta(days=0), timedelta(days=5)),
    ],
)
async def test_reset_personal_token(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    since_delta: timedelta,
    until_delta: timedelta,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()

    old_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "test",
            "since": today + since_delta,
            "until": today + until_delta,
        },
    )
    async_session.expunge(old_token)

    until = today + timedelta(days=366)

    before = datetime.now(tz=UTC)
    response = await test_client.patch(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={},
    )
    after = datetime.now(tz=UTC)

    assert response.status_code == HTTPStatus.OK, response.json()

    query = (
        select(PersonalToken)
        .where(PersonalToken.user_id == mocked_user.user.id, PersonalToken.name == "test")
        .order_by(PersonalToken.revoked_at.nulls_first(), PersonalToken.id)
    )
    personal_token_scalars = await async_session.scalars(query)
    personal_tokens = list(personal_token_scalars.all())

    assert len(personal_tokens) == 2
    new_token, revoked_token = personal_tokens

    # new token with exact same data was created, with with new dates
    assert new_token.id != old_token.id
    assert new_token.name == old_token.name
    assert new_token.user_id == old_token.user_id
    assert new_token.since == today
    assert new_token.until == until
    assert new_token.revoked_at is None

    # old token was revoked
    assert revoked_token.id == old_token.id
    assert revoked_token.revoked_at is not None
    assert before <= revoked_token.revoked_at <= after

    response_data = response.json()
    assert response_data.keys() == {"id", "data", "content"}
    assert response_data["id"] == str(new_token.id)
    assert response_data["data"] == personal_token_to_json(new_token)

    # only new token is returned
    jwt = response_data["content"]
    assert jwt
    assert personal_token_jwt_decoder(jwt) == {
        "jti": str(new_token.id),
        "iss": "data-rentgen",
        "token_name": "test",
        "sub_id": mocked_user.user.id,
        "preferred_username": mocked_user.user.name,
        "scope": "all:read all:write",
        "iat": int(extract_timestamp_from_uuid(new_token.id).timestamp()),
        "nbf": day_beginning(new_token.since).timestamp(),
        "exp": day_beginning(new_token.until + timedelta(days=1)).timestamp(),
    }


async def test_reset_personal_token_with_until(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    personal_token_jwt_decoder: Callable[[str], dict],
):
    today = datetime.now(tz=UTC).date()

    old_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "test",
            "since": today - timedelta(days=10),
            "until": today + timedelta(days=5),
        },
    )
    async_session.expunge(old_token)

    until = today + timedelta(days=1)

    before = datetime.now(tz=UTC)
    response = await test_client.patch(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"until": until.isoformat()},
    )
    after = datetime.now(tz=UTC)

    assert response.status_code == HTTPStatus.OK, response.json()

    query = (
        select(PersonalToken)
        .where(PersonalToken.user_id == mocked_user.user.id, PersonalToken.name == "test")
        .order_by(PersonalToken.revoked_at.nulls_first(), PersonalToken.id)
    )
    personal_token_scalars = await async_session.scalars(query)
    personal_tokens = list(personal_token_scalars.all())

    assert len(personal_tokens) == 2
    new_token, revoked_token = personal_tokens

    # new token with exact same data was created, with with new dates
    assert new_token.id != old_token.id
    assert new_token.name == old_token.name
    assert new_token.user_id == old_token.user_id
    assert new_token.since == today
    assert new_token.until == until
    assert new_token.revoked_at is None

    # old token was revoked
    assert revoked_token.id == old_token.id
    assert revoked_token.revoked_at is not None
    assert before <= revoked_token.revoked_at <= after

    response_data = response.json()
    assert response_data.keys() == {"id", "data", "content"}
    assert response_data["id"] == str(new_token.id)
    assert response_data["data"] == personal_token_to_json(new_token)

    # only new token is returned
    jwt = response_data["content"]
    assert jwt
    assert personal_token_jwt_decoder(jwt) == {
        "jti": str(new_token.id),
        "iss": "data-rentgen",
        "token_name": "test",
        "sub_id": mocked_user.user.id,
        "preferred_username": mocked_user.user.name,
        "scope": "all:read all:write",
        "iat": int(extract_timestamp_from_uuid(new_token.id).timestamp()),
        "nbf": day_beginning(new_token.since).timestamp(),
        "exp": day_beginning(new_token.until + timedelta(days=1)).timestamp(),
    }


async def test_reset_personal_token_with_until_more_than_max_value(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()

    old_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "test",
            "since": today - timedelta(days=10),
            "until": today + timedelta(days=5),
        },
    )

    until = today + timedelta(days=1000)
    max_until = today + timedelta(days=366)

    response = await test_client.patch(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"until": until.isoformat()},
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


async def test_reset_personal_token_with_until_in_past(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    until = today - timedelta(days=1)

    old_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "test",
            "since": today - timedelta(days=10),
            "until": today + timedelta(days=5),
        },
    )

    response = await test_client.patch(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={"until": until.isoformat()},
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


async def test_reset_personal_token_already_revoked(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    old_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "name": "test",
            "since": today - timedelta(days=10),
            "until": today + timedelta(days=5),
            "revoked_at": today - timedelta(days=1),
        },
    )

    response = await test_client.patch(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
    assert response.json() == {
        "error": {
            "code": "not_found",
            "message": f"PersonalToken with id='{old_token.id}' not found",
            "details": {
                "entity_type": "PersonalToken",
                "field": "id",
                "value": str(old_token.id),
            },
        },
    }


async def test_reset_personal_token_not_found(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
):
    today = datetime.now(tz=UTC).date()
    revoked_token = await create_personal_token(
        async_session,
        mocked_user.user,
        token_kwargs={
            "revoked_at": today - timedelta(days=1),
        },
    )

    another_user = await create_user(async_session)
    foreign_token = await create_personal_token(async_session, another_user)

    fake_token = personal_token_factory()

    for token in [revoked_token, foreign_token, fake_token]:
        response = await test_client.patch(
            f"v1/personal-tokens/{token.id}",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
            json={},
        )

        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
        assert response.json() == {
            "error": {
                "code": "not_found",
                "message": f"PersonalToken with id='{token.id}' not found",
                "details": {
                    "entity_type": "PersonalToken",
                    "field": "id",
                    "value": str(token.id),
                },
            },
        }


async def test_reset_personal_token_unauthorized(
    test_client: AsyncClient,
):
    token = personal_token_factory()

    response = await test_client.patch(f"v1/personal-tokens/{token.id}", json={})

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
async def test_reset_personal_token_disabled(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    server_app_settings: Settings,
):
    token = await create_personal_token(async_session, mocked_user.user)

    response = await test_client.patch(
        f"v1/personal-tokens/{token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
        json={},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN, response.json()
    assert response.json() == {
        "error": {
            "code": "forbidden",
            "message": "Action not allowed",
            "details": "Authentication using PersonalTokens is disabled",
        },
    }


async def test_reset_personal_token_via_personal_token_not_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    personal_token: PersonalToken,
):
    response = await test_client.patch(
        f"v1/personal-tokens/{personal_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
        json={},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "PersonalToken was passed but access token was expected",
        },
    }
