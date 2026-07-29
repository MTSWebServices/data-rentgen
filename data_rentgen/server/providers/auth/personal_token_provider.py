# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from datetime import UTC, datetime, timedelta
from pprint import pformat
from typing import Literal
from uuid import UUID

from cachetools import LRUCache, TTLCache
from fastapi import FastAPI, Request

from data_rentgen.db.models import PersonalToken, User
from data_rentgen.exceptions.auth import AuthorizationError
from data_rentgen.exceptions.entity import EntityNotFoundError
from data_rentgen.server.services.personal_token import PersonalTokenService
from data_rentgen.server.settings.auth.personal_token import PersonalTokenSettings
from data_rentgen.server.utils.jwt import decode_jwt, sign_jwt
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils.uuid import extract_timestamp_from_uuid

logger = logging.getLogger(__name__)


class PersonalTokenCache:
    def __init__(self, maxsize: int, ttl_seconds: int):
        self.alive_cache: TTLCache[UUID, Literal[True]] = TTLCache(
            maxsize=maxsize,
            ttl=ttl_seconds,
        )
        # revoked tokens cannot be resurrected, no TTL
        self.revoked_cache: LRUCache[UUID, Literal[True]] = LRUCache(maxsize=maxsize)

    def is_revoked(self, token_id: UUID) -> bool | None:
        if token_id in self.revoked_cache:
            return True
        if token_id in self.alive_cache:
            return False
        return None

    def add_token(self, token_id: UUID) -> None:
        self.alive_cache[token_id] = True

    def revoke_token(self, token_id: UUID) -> None:
        self.revoked_cache[token_id] = True
        self.alive_cache.pop(token_id, None)


class PersonalTokenAuthProvider:
    """Auth provider which only accepts personal tokens.

    Used internally by DataRentgen. Cannot not be used as a regular AuthProvider.
    """

    def __init__(
        self,
        settings: PersonalTokenSettings,
    ) -> None:
        self._settings = settings
        self._token_cache = PersonalTokenCache(
            maxsize=settings.cache_size,
            ttl_seconds=settings.cache_ttl_seconds,
        )

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = PersonalTokenSettings.model_validate(
            app.state.settings.auth.personal_tokens,
        )
        logger.info("Initializing %s provider with settings:\n%s", cls.__name__, pformat(settings))
        app.state.personal_token_auth_provider = cls(settings)
        return app

    def generate_jwt(
        self,
        user: User,
        token: PersonalToken,
    ) -> str:
        since = datetime.combine(token.since, datetime.min.time(), tzinfo=UTC)
        until = datetime.combine(token.until, datetime.min.time(), tzinfo=UTC) + timedelta(days=1)
        claims = {
            "jti": str(token.id),
            "token_name": token.name,
            "sub_id": user.id,
            "preferred_username": user.name,
            "scope": " ".join(token.scopes),
            "iat": int(extract_timestamp_from_uuid(token.id).timestamp()),
            "nbf": since.timestamp(),
            "exp": until.timestamp(),
        }
        jwt = sign_jwt(
            payload=claims,
            secret_key=self._settings.secret_key.get_secret_value(),  # type: ignore[union-attr]
            security_algorithm=self._settings.security_algorithm,
        )
        return f"personal_token_{jwt}"

    def extract_token(
        self,
        jwt_string: str,
    ) -> tuple[User, PersonalToken]:
        claims = decode_jwt(
            token=jwt_string.replace("personal_token_", ""),
            secret_key=self._settings.secret_key.get_secret_value(),  # type: ignore[union-attr]
            security_algorithm=self._settings.security_algorithm,
        )
        try:
            token = PersonalToken(
                id=UUID(claims["jti"]),
                since=datetime.fromtimestamp(float(claims["nbf"]), tz=UTC),
                until=datetime.fromtimestamp(float(claims["exp"]), tz=UTC),
                name=claims["token_name"],
                user_id=claims["sub_id"],
                scopes=claims["scope"].split(" "),
            )
            user = User(id=int(claims["sub_id"]), name=claims["preferred_username"])
        except (KeyError, TypeError, ValueError) as e:
            msg = "Invalid token"
            raise AuthorizationError(msg) from e
        return user, token

    async def get_current_user(self, access_token: str | None, request: Request) -> User:
        if not access_token:
            msg = "Missing Authorization header"
            raise AuthorizationError(msg)

        if not access_token.startswith("personal_token_"):
            details = "Access token was passed but PersonalToken was expected"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)

        if not self._settings.enabled:
            msg = "Authentication using PersonalTokens is disabled"
            raise AuthorizationError(msg)

        user, token = self.extract_token(access_token)
        is_revoked = await self.check_token_revoked(user, token, request)
        if is_revoked:
            details = f"PersonalToken name='{token.name}', id={token.id} is revoked"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)
        logger.debug("Got user %r from token %r", user, token)
        return user

    async def get_optional_user(self, access_token: str | None, request: Request) -> User | None:
        if not access_token or not access_token.startswith("personal_token_"):
            return None

        if not self._settings.enabled:
            msg = "Authentication using PersonalTokens is disabled"
            raise AuthorizationError(msg)

        user, token = self.extract_token(access_token)
        is_revoked = await self.check_token_revoked(user, token, request)
        if is_revoked:
            details = f"PersonalToken name='{token.name}', id={token.id} is revoked"
            msg = "Invalid token"
            raise AuthorizationError(msg, details)
        logger.debug("Got user %r from token %r", user, token)
        return user

    async def check_token_revoked(self, user: User, token: PersonalToken, request: Request) -> bool:
        is_revoked = self._token_cache.is_revoked(token.id)
        if is_revoked is not None:
            return is_revoked

        is_revoked = False

        # checking session in cache is fast, creating new session is slow,
        # let's postpone it using a hack
        async with request.app.state.session_factory() as session:
            personal_token_service = PersonalTokenService(
                uow=UnitOfWork(session),
                request=request,
            )
            try:
                await personal_token_service.get(user, token.id)
                is_revoked = False
            except EntityNotFoundError:
                is_revoked = True

        if is_revoked:
            self._token_cache.revoke_token(token.id)
        else:
            self._token_cache.add_token(token.id)
        return is_revoked
