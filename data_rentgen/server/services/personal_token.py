# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import Depends, Request

from data_rentgen.db.models import PersonalToken, User
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.exceptions import ActionNotAllowedError, EntityNotFoundError
from data_rentgen.services.uow import UnitOfWork
from data_rentgen.utils.uuid import generate_new_uuid

if TYPE_CHECKING:
    from data_rentgen.server.settings.auth.personal_token import PersonalTokenSettings


@dataclass(slots=True)
class PersonalTokenServiceResult:
    id: UUID
    data: PersonalToken


class PersonalTokenServicePaginatedResult(PaginationDTO[PersonalTokenServiceResult]):
    pass


class PersonalTokenService:
    def __init__(
        self,
        uow: Annotated[UnitOfWork, Depends()],
        request: Request,
    ):
        self._uow = uow
        self._settings: PersonalTokenSettings = request.app.state.settings.auth.personal_tokens

    async def __aenter__(self):
        await self._uow.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._uow.__aexit__(exc_type, exc_value, traceback)

    async def paginate(
        self,
        page: int,
        page_size: int,
        user: User,
        personal_token_ids: list[UUID],
    ) -> PersonalTokenServicePaginatedResult:
        pagination = await self._uow.personal_token.paginate(
            page=page,
            page_size=page_size,
            user_id=user.id,
            personal_token_ids=personal_token_ids,
        )

        return PersonalTokenServicePaginatedResult(
            page=pagination.page,
            page_size=pagination.page_size,
            total_count=pagination.total_count,
            items=[PersonalTokenServiceResult(id=token.id, data=token) for token in pagination.items],
        )

    async def get(
        self,
        user: User,
        token_id: UUID,
    ) -> PersonalToken:
        token = await self._uow.personal_token.get_by_id(user.id, token_id)
        if not token:
            raise EntityNotFoundError("PersonalToken", "id", str(token_id))  # noqa: EM101
        return token

    async def create(
        self,
        user: User,
        name: str,
        scopes: list[str],
        until: date | None,
    ) -> PersonalToken:
        if not self._settings.enabled:
            error_msg = "Authentication using PersonalTokens is disabled"
            raise ActionNotAllowedError(error_msg)

        today = datetime.now(tz=UTC).date()
        max_until = today + timedelta(days=self._settings.max_duration_days)
        until = min(max_until, until or max_until)

        return await self._uow.personal_token.create(
            token_id=generate_new_uuid(),
            user_id=user.id,
            name=name,
            scopes=scopes,
            since=today,
            until=until,
        )

    async def revoke(
        self,
        user: User,
        token_id: UUID,
    ) -> PersonalToken:
        token = await self.get(user, token_id)
        return await self._uow.personal_token.revoke(token)
