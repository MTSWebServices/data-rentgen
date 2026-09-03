import time
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from data_rentgen.db.models import PersonalToken, User
from data_rentgen.server.settings import ServerApplicationSettings
from data_rentgen.server.settings.auth.jwt import JWTSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt


@dataclass(slots=True)
class MockedUser:
    user: User
    access_token: str
    personal_token: str


@pytest.fixture
def mocked_user(
    user: User,
    personal_token: PersonalToken,
    access_token_generator: Callable[[User], str],
    personal_token_jwt_generator: Callable[[PersonalToken, User], str],
) -> MockedUser:
    return MockedUser(
        user=user,
        access_token=access_token_generator(user),
        personal_token=personal_token_jwt_generator(personal_token, user),
    )


@pytest.fixture
def access_token_settings(server_app_settings: ServerApplicationSettings) -> JWTSettings:
    return JWTSettings.model_validate(server_app_settings.auth.access_token)


@pytest.fixture
def access_token_generator(access_token_settings: JWTSettings) -> Callable[[User], str]:
    def _generate_access_token(user: User, time_delta: int = 1000) -> str:
        now = time.time()
        jwt = sign_jwt(
            {
                "iss": "data-rentgen",
                "sub_id": user.id,
                "preferred_username": user.name,
                "iat": 0,
                "nbf": 0,
                "exp": now + time_delta,
            },
            access_token_settings.secret_key.get_secret_value(),
            access_token_settings.security_algorithm,
        )
        return f"access_token_{jwt}"

    return _generate_access_token


@pytest.fixture
def access_token_jwt_decoder(access_token_settings: JWTSettings) -> Callable[[str], dict]:
    def _decoder(jwt_string: str) -> dict:
        return decode_jwt(
            token=jwt_string.replace("access_token_", ""),
            secret_key=access_token_settings.secret_key.get_secret_value(),
            security_algorithm=access_token_settings.security_algorithm,
        )

    return _decoder
