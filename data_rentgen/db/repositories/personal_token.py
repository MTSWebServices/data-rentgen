# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, date, datetime
from uuid import UUID

from sqlalchemy import any_, bindparam, literal, select
from sqlalchemy.exc import IntegrityError

from data_rentgen.db.models import PersonalToken
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto.pagination import PaginationDTO
from data_rentgen.exceptions.entity import EntityAlreadyExistsError, EntityNotFoundError

get_by_id_query = (
    select(PersonalToken)
    .where(
        PersonalToken.user_id == bindparam("user_id"),
        PersonalToken.id == bindparam("token_id"),
        PersonalToken.revoked_at.is_(None),
    )
    .order_by(PersonalToken.id)
    .limit(literal(1, literal_execute=True))
)


class PersonalTokenRepository(Repository[PersonalToken]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        user_id: int,
        personal_token_ids: list[UUID],
    ) -> PaginationDTO[PersonalToken]:
        where = [
            PersonalToken.user_id == user_id,
            PersonalToken.revoked_at.is_(None),
        ]
        limit = 0

        if len(personal_token_ids) == 1:
            where.append(PersonalToken.id == personal_token_ids[0])
            limit = 1
        elif personal_token_ids:
            where.append(PersonalToken.id == any_(list(personal_token_ids)))  # type: ignore[arg-type]
            limit = len(personal_token_ids)

        query = select(PersonalToken).distinct(PersonalToken.name).where(*where)

        return await self._paginate_by_query(
            query=query,
            order_by=[PersonalToken.name, PersonalToken.since.desc()],
            page=page,
            page_size=page_size,
            override_limit=limit,
        )

    async def get_by_id(
        self,
        user_id: int,
        token_id: UUID,
    ) -> PersonalToken | None:
        return await self._session.scalar(
            get_by_id_query,
            {"user_id": user_id, "token_id": token_id},
        )

    async def create(
        self,
        *,
        token_id: UUID,
        user_id: int,
        name: str,
        scopes: list[str],
        since: date,
        until: date,
    ) -> PersonalToken:
        result = PersonalToken(
            id=token_id,
            user_id=user_id,
            name=name,
            scopes=scopes,
            since=since,
            until=until,
        )
        self._session.add(result)
        try:
            await self._session.flush([result])
        except IntegrityError as e:
            constraint = e.__cause__.__cause__.constraint_name  # type: ignore[union-attr]
            if constraint == "uq__personal_token__user_id_since_until_name":
                raise EntityAlreadyExistsError("PersonalToken", "name", name) from e  # noqa: EM101
            if constraint == "fk__personal_token__user_id__user":
                raise EntityNotFoundError("User", "id", user_id) from e  # noqa: EM101
            raise
        return result

    async def revoke(self, user_token: PersonalToken) -> PersonalToken:
        user_token.revoked_at = datetime.now(tz=UTC)
        await self._session.flush([user_token])
        return user_token
