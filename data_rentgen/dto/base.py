# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Hashable
from typing import Protocol, Self


class DTO(Protocol):
    @property
    def unique_key(self) -> Hashable:
        """Expected to return the same value for the same DTO object"""
        ...

    def merge(self, new: Self) -> Self:
        """
        Expected to update existing object with data from new object
        (from different point in time).

        unique_key should be exactly the same!
        """
        ...
