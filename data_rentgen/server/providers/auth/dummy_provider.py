# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from pprint import pformat
from time import time
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Request

from data_rentgen.db.models import User
from data_rentgen.dependencies import Stub
from data_rentgen.dto import UserDTO
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.server.providers.auth.base_provider import AuthProvider
from data_rentgen.server.settings.auth.dummy import DummyAuthProviderSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt
from data_rentgen.services import UnitOfWork

logger = logging.getLogger(__name__)


class DummyAuthProvider(AuthProvider):
    """Auth provider which accepts any user and password."""

    def __init__(
        self,
        settings: Annotated[DummyAuthProviderSettings, Depends(Stub(DummyAuthProviderSettings))],
        unit_of_work: Annotated[UnitOfWork, Depends()],
    ) -> None:
        self._settings = settings
        self._uow = unit_of_work

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = DummyAuthProviderSettings.model_validate(
            app.state.settings.auth.model_dump(exclude={"provider"}),
        )
        logger.info("Using %s provider with settings:\n%s", cls.__name__, pformat(settings))

        async def get_settings():
            return settings

        app.dependency_overrides[AuthProvider] = cls
        app.dependency_overrides[DummyAuthProviderSettings] = get_settings
        return app

    def generate_jwt(
        self,
        user: User,
    ) -> tuple[str, float]:
        now = time()
        expires_at = time() + self._settings.access_token.expire_seconds
        claims = {
            "sub_id": user.id,
            "preferred_username": user.name,
            "iat": now,
            "nbf": now,
            "exp": expires_at,
        }
        jwt = sign_jwt(
            payload=claims,
            secret_key=self._settings.access_token.secret_key.get_secret_value(),
            security_algorithm=self._settings.access_token.security_algorithm,
        )
        return f"access_token_{jwt}", expires_at

    def extract_token(
        self,
        jwt_string: str,
    ) -> User:
        claims = decode_jwt(
            token=jwt_string.replace("access_token_", ""),
            secret_key=self._settings.access_token.secret_key.get_secret_value(),
            security_algorithm=self._settings.access_token.security_algorithm,
        )
        try:
            return User(id=int(claims["sub_id"]), name=claims["preferred_username"])
        except (KeyError, TypeError, ValueError) as e:
            msg = "Invalid token"
            raise AuthorizationError(msg) from e

    async def get_current_user(self, access_token: str | None, request: Request) -> User:
        if not access_token:
            msg = "Missing Authorization header"
            raise AuthorizationError(msg)
        if not access_token.startswith("access_token_"):
            details = "PersonalToken was passed but access token was expected"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)
        return self.extract_token(access_token)

    async def get_token_password_grant(
        self,
        login: str,
        password: str,
    ) -> dict[str, Any]:
        if not login:
            msg = "Missing auth credentials"
            raise AuthorizationError(msg)

        logger.debug("Get/create user %r in database", login)
        async with self._uow:
            user = await self._uow.user.get_or_create(UserDTO(name=login))

        logger.debug("User with id %r found", user.id)
        logger.debug("Generate access token for user id %r", user.id)
        access_token, expires_at = self.generate_jwt(user)
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_at": expires_at,
        }

    async def get_token_authorization_code_grant(
        self,
        code: str,
    ) -> dict[str, Any]:
        msg = "Authorization code grant is not supported by DummyAuthProvider"
        raise NotImplementedError(msg)

    async def logout(self, user: User, request: Request) -> None:
        msg = "Logout method is not implemented for DummyAuthProvider"
        raise NotImplementedError(msg)
