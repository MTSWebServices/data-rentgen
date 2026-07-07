# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Any

from data_rentgen.exceptions.base import ApplicationError


class EntityNotFoundError(ApplicationError):
    """Entity not found.

    Examples
    --------

    ```python
    >>> from data_rentgen.exceptions import EntityNotFoundError
    >>> raise EntityNotFoundError("User", "username", "test")
    Traceback (most recent call last):
    data_rentgen.exceptions.entity.EntityNotFoundError: User with username='test' not found
    ```
    """

    entity_type: str
    """Entity type"""

    field: str
    """Entity identifier field"""

    value: Any
    """Entity identifier value"""

    def __init__(self, entity_type: str, field: str, value: Any):
        self.entity_type = entity_type
        self.field = field
        self.value = value

    @property
    def message(self) -> str:
        return f"{self.entity_type} with {self.field}={self.value!r} not found"

    @property
    def details(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "field": self.field,
            "value": self.value,
        }


class EntityAlreadyExistsError(ApplicationError):
    """Entity already exists.

    Examples
    --------

    ```python
    >>> from data_rentgen.exceptions import EntityNotFoundError
    >>> raise EntityAlreadyExistsError("User", "username", "test")
    Traceback (most recent call last):
    data_rentgen.exceptions.entity.EntityAlreadyExistsError: User with username='test' already exists
    ```
    """

    entity_type: str
    """Entity type"""

    field: str
    """Entity identifier field"""

    value: Any
    """Entity identifier value"""

    def __init__(self, entity_type: str, field: str, value: Any):
        self.entity_type = entity_type
        self.field = field
        self.value = value

    @property
    def message(self) -> str:
        return f"{self.entity_type} with {self.field}={self.value!r} already exists"

    @property
    def details(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "field": self.field,
            "value": self.value,
        }
