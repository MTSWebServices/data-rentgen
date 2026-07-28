# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Any

from fastapi import FastAPI, Request

from data_rentgen.db.models import User


class AuthProvider(ABC):
    """Basic class for all Auth providers.

    Constructor is called by FastAPI, and can use Dependency injection mechanism.
    See [setup][] for more details.
    """

    @classmethod
    @abstractmethod
    def setup(cls, app: FastAPI) -> FastAPI:
        """
        This method is called by [data_rentgen.server.application_factory][].

        Here you should add dependency overrides for auth provider,
        and return new `app` object.

        Examples
        --------

        ```python
        from fastapi import FastAPI
        from my_awesome_auth_provider.settings import MyAwesomeAuthProviderSettings
        from data_rentgen.dependencies import Stub

        class MyAwesomeAuthProvider(AuthProvider):
            def setup(app):
                app.dependency_overrides[AuthProvider] = MyAwesomeAuthProvider

                # `settings_object_factory` returns MyAwesomeAuthProviderSettings object
                app.dependency_overrides[MyAwesomeAuthProviderSettings] = settings_object_factory
                return app

            def __init__(
                self,
                settings: Annotated[MyAwesomeAuthProviderSettings, Depends(Stub(MyAwesomeAuthProviderSettings))],
            ):
                # settings object is set automatically by FastAPI's dependency_overrides
                self.settings = settings
        ```
        """
        ...

    @abstractmethod
    async def get_current_user(self, access_token: str | None, request: Request) -> User:
        """
        This method should return currently logged in user.

        Parameters
        ----------
        access_token : str
            JWT token got from `Authorization: Bearer <token>` header.

        Returns
        -------
        User
            Current user object
        """
        ...

    @abstractmethod
    async def get_token_password_grant(
        self,
        login: str,
        password: str,
    ) -> dict[str, Any]:
        """
        This method should perform authentication and return JWT token.

        Parameters
        ----------
        login : str
            User's login name.
        password : str
            User's password.

        Returns
        -------
        Dict:
            ```python
            {
                "access_token": "some.jwt.token",
                "token_type": "bearer",
                "expires_in": 3600,
            }
            ```

        Notes
        -----
        See:

        * https://auth0.com/docs/get-started/authentication-and-authorization-flow/call-your-api-using-resource-owner-password-flow
        * https://connect2id.com/products/server/docs/api/token
        """
        ...

    @abstractmethod
    async def get_token_authorization_code_grant(
        self,
        code: str,
    ) -> dict[str, Any]:
        """
        Obtain a token using the Authorization Code grant.
        """

    @abstractmethod
    async def logout(self, user: User, request: Request) -> None:
        """This method should implement user logout logic"""
