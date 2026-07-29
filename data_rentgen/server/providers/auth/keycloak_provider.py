# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import time
from typing import Any, NoReturn

from fastapi import FastAPI, Request
from jwcrypto import jwk
from jwcrypto.common import JWException
from keycloak import KeycloakOpenID, KeycloakOperationError
from starlette.middleware.sessions import SessionMiddleware

from data_rentgen.db.models import User
from data_rentgen.dto import UserDTO
from data_rentgen.exceptions.auth import AuthorizationError, LogoutError
from data_rentgen.exceptions.redirect import RedirectError
from data_rentgen.server.providers.auth.base_provider import AuthProvider
from data_rentgen.server.settings.auth.keycloak import KeycloakAuthProviderSettings
from data_rentgen.services import UnitOfWork

logger = logging.getLogger(__name__)


class KeycloakAuthProvider(AuthProvider):
    """Auth provider which uses Keycloak OpenID Connect."""

    def __init__(
        self,
        settings: KeycloakAuthProviderSettings,
    ) -> None:
        self.settings = settings
        self.keycloak_openid = KeycloakOpenID(
            server_url=str(self.settings.keycloak.api_url).rstrip("/") + "/",
            client_id=self.settings.keycloak.client_id,
            realm_name=self.settings.keycloak.realm_name,
            client_secret_key=self.settings.keycloak.client_secret.get_secret_value(),
            verify=self.settings.keycloak.verify_ssl,
        )
        self._key: jwk.JWKSet | None = None
        self._key_expiration: float = time.monotonic()

    @classmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        settings = KeycloakAuthProviderSettings.model_validate(
            app.state.settings.auth.model_dump(exclude={"provider"}),
        )
        logger.info("Using %s provider with settings:\n%s", cls.__name__, settings)

        app.state.auth_provider = cls(settings)
        app.add_middleware(
            SessionMiddleware,
            secret_key=settings.cookie.secret_key.get_secret_value(),
            session_cookie=settings.cookie.name,
            **settings.cookie.model_dump(exclude={"secret_key", "name"}),
        )
        return app

    async def get_token_password_grant(
        self,
        login: str,
        password: str,
        uow: UnitOfWork,
    ) -> dict[str, Any]:
        msg = "Password grant is not supported by KeycloakAuthProvider"
        raise NotImplementedError(msg)

    async def get_token_authorization_code_grant(
        self,
        code: str,
        request: Request,
    ) -> dict[str, Any]:
        try:
            return await self.keycloak_openid.a_token(
                grant_type="authorization_code",
                code=code,
                redirect_uri=self.settings.keycloak.ui_callback_url,
            )
        except KeycloakOperationError as e:
            logger.exception("Fail to get token from keycloak: %s", e)  # noqa: TRY401
            msg = "Failed to get token"
            raise AuthorizationError(msg) from e

    async def get_current_user(self, access_token: str | None, request: Request, uow: UnitOfWork) -> User:
        # we ignore explicit token passed via Authorization header
        access_token = request.session.get("access_token")
        if not access_token:
            logger.debug("No access token found in session")
            await self.redirect_to_auth()

        # if user is disabled or blocked in Keycloak after the token is issued, he will
        # remain authorized until the token expires (not more than 15 minutes in MTS SSO)
        token_info = await self.decode_token(access_token)
        refresh_token = request.session.get("refresh_token")

        if token_info is None and refresh_token:
            logger.debug("Access token invalid. Attempting to refresh.")
            access_token, refresh_token = await self.refresh_access_token(refresh_token)
            token_info = await self.decode_token(access_token)
            request.session["access_token"] = access_token
            request.session["refresh_token"] = refresh_token

        if token_info is None:
            await self.redirect_to_auth()

        # field name is hardcoded in keycloak:
        # https://github.com/keycloak/keycloak/blob/3ca3a4ad349b4d457f6829eaf2ae05f1e01408be/core/src/main/java/org/keycloak/representations/IDToken.java
        login = token_info.get("preferred_username")
        if not login:
            msg = "Invalid token"
            raise AuthorizationError(msg)

        async with uow:
            return await uow.user.get_or_create(UserDTO(name=login))

    async def _get_key(self) -> jwk.JWKSet:
        # avoid sending requests to Keycloak for every received token
        if self._key is not None and self._key_expiration > time.monotonic():
            return self._key

        key = jwk.JWKSet()
        certs = await self.keycloak_openid.a_certs()
        for cert in certs["keys"]:
            key.add(jwk.JWK(**cert))

        self._key = key
        self._key_expiration = time.monotonic() + self.settings.keycloak.cert_cache_ttl.total_seconds()
        return self._key

    async def decode_token(self, access_token: str) -> dict[str, Any] | None:
        try:
            key = await self._get_key()
            return await self.keycloak_openid.a_decode_token(token=access_token, key=key)
        except (KeycloakOperationError, JWException) as err:
            logger.debug("Access token is invalid or expired: %s", err)
            return None

    async def refresh_access_token(self, refresh_token: str) -> tuple[str, str]:
        try:
            new_tokens = await self.keycloak_openid.a_refresh_token(refresh_token)
            logger.debug("Access token refreshed")
            return new_tokens["access_token"], new_tokens["refresh_token"]
        except (KeycloakOperationError, JWException) as err:
            logger.debug("Failed to refresh access token: %s", err)
            await self.redirect_to_auth()

    async def redirect_to_auth(self) -> NoReturn:
        auth_url = await self.keycloak_openid.a_auth_url(
            redirect_uri=self.settings.keycloak.ui_callback_url,
            scope=self.settings.keycloak.scope,
        )

        logger.debug("Redirecting user to auth url: %s", auth_url)
        raise RedirectError(
            message="Please authorize using provided URL",
            details=auth_url,
        )

    async def logout(self, user: User, request: Request) -> None:
        request.session.pop("access_token", None)

        refresh_token = request.session.pop("refresh_token", None)
        if not refresh_token:
            logger.debug("No refresh token found in session.")
            return

        try:
            await self.keycloak_openid.a_logout(refresh_token)
        except KeycloakOperationError as err:
            logger.debug("Can't logout user %s: %s", user.name, err)
            msg = f"Can't logout user: {user.name}"
            raise LogoutError(msg) from err
