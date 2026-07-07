# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from data_rentgen.exceptions.base import ApplicationError


class AuthorizationError(ApplicationError):
    """Authorization request is failed.

    Examples
    --------

    ```python
    >>> from data_rentgen.exceptions import AuthorizationError
    >>> raise AuthorizationError("User 'test' is disabled")
    Traceback (most recent call last):
    data_rentgen.exceptions.auth.AuthorizationError: User 'test' is disabled
    ```
    """

    def __init__(self, message: str, details: Any = None) -> None:
        self._message = message
        self._details = details

    @property
    def message(self) -> str:
        return self._message

    @property
    def details(self) -> Any:
        return self._details


class LogoutError(ApplicationError):
    """Error on logout request"""

    def __init__(self, details: str) -> None:
        self._message = "Logout error"
        self._details = details

    @property
    def message(self) -> str:
        return self._message

    @property
    def details(self) -> Any:
        return self._details


class ActionNotAllowedError(ApplicationError):
    """Action is not allowed for current user.

    Examples
    --------

    ```python
    >>> from data_rentgen.exceptions import ActionNotAllowedError
    >>> raise ActionNotAllowedError("User 'test' cannot perform some action")
    Traceback (most recent call last):
    data_rentgen.exceptions.auth.ActionNotAllowedError: User 'test' cannot perform some action
    ```
    """

    def __init__(self, details: str) -> None:
        self._message = "Action not allowed"
        self._details = details

    @property
    def message(self) -> str:
        return self._message

    @property
    def details(self) -> Any:
        return self._details
