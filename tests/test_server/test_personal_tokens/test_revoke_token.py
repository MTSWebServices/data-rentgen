from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus

import pytest
from httpx2 import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.db.models.personal_token import PersonalToken
from tests.fixtures.mocks import MockedUser
from tests.test_server.fixtures.factories.personal_token import (
    create_personal_token,
    personal_token_factory,
)
from tests.test_server.fixtures.factories.user import create_user

pytestmark = [pytest.mark.server, pytest.mark.asyncio]


def day_beginning(inp: date) -> datetime:
    return datetime.combine(inp, datetime.min.time(), tzinfo=UTC)


# user can revoke any token, including expired
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
async def test_revoke_personal_token(
    test_client: AsyncClient,
    async_session: AsyncSession,
    mocked_user: MockedUser,
    since_delta: timedelta,
    until_delta: timedelta,
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

    before = datetime.now(tz=UTC)
    response = await test_client.delete(
        f"v1/personal-tokens/{old_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.access_token}"},
    )
    after = datetime.now(tz=UTC)

    assert response.status_code == HTTPStatus.NO_CONTENT, response.json()

    query = select(PersonalToken).where(PersonalToken.user_id == mocked_user.user.id, PersonalToken.name == "test")
    personal_token_scalars = await async_session.scalars(query)
    personal_tokens = list(personal_token_scalars.all())

    assert len(personal_tokens) == 1
    [revoked_token] = personal_tokens

    assert revoked_token.id == old_token.id
    assert revoked_token.revoked_at is not None
    assert before <= revoked_token.revoked_at <= after


async def test_revoke_personal_token_not_found(
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
        response = await test_client.delete(
            f"v1/personal-tokens/{token.id}",
            headers={"Authorization": f"Bearer {mocked_user.access_token}"},
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


async def test_revoke_personal_token_unauthorized(
    test_client: AsyncClient,
):
    token = personal_token_factory()

    response = await test_client.delete(f"v1/personal-tokens/{token.id}")

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Missing Authorization header",
            "details": None,
        },
    }


async def test_revoke_personal_token_via_personal_token_not_allowed(
    test_client: AsyncClient,
    mocked_user: MockedUser,
    personal_token: PersonalToken,
):
    response = await test_client.delete(
        f"v1/personal-tokens/{personal_token.id}",
        headers={"Authorization": f"Bearer {mocked_user.personal_token}"},
    )

    assert response.status_code == HTTPStatus.UNAUTHORIZED, response.json()
    assert response.json() == {
        "error": {
            "code": "unauthorized",
            "message": "Invalid token",
            "details": "PersonalToken was passed but access token was expected",
        },
    }
