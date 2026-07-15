# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import any_, bindparam, literal, select

from data_rentgen.db.models import User
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import UserDTO

fetch_bulk_query = (
    select(User)
    .where(
        User.name_lower == any_(bindparam("names_lower")),
    )
    .limit(bindparam("limit"))
)

get_one_by_name_query = (
    select(User)
    .where(
        User.name_lower == bindparam("name_lower"),
    )
    .limit(literal(1, literal_execute=True))
)

get_one_by_id_query = (
    select(User)
    .where(
        User.id == bindparam("id"),
    )
    .limit(literal(1, literal_execute=True))
)


class UserRepository(Repository[User]):
    async def fetch_bulk(self, users_dto: list[UserDTO]) -> list[tuple[UserDTO, User | None]]:
        if not users_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "names_lower": [item.name.lower() for item in users_dto],
                "limit": len(users_dto),
            },
        )
        existing = {user.name.lower(): user for user in scalars.all()}
        return [(user_dto, existing.get(user_dto.name.lower())) for user_dto in users_dto]

    async def create(self, user_dto: UserDTO) -> User:
        # if another worker already created the same row, just use it. if not - create with holding the lock.
        await self._lock(user_dto.name)
        return await self._get(user_dto.name) or await self._create(user_dto)

    async def get_or_create(self, user_dto: UserDTO) -> User:
        result = await self._get(user_dto.name)
        if result:
            return result

        await self._lock(user_dto.name)
        return await self._get(user_dto.name) or await self._create(user_dto)

    async def _get(self, name: str) -> User | None:
        return await self._session.scalar(get_one_by_name_query, {"name_lower": name.lower()})

    async def _create(self, user: UserDTO) -> User:
        result = User(name=user.name)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def read_by_id(self, id_: int) -> User | None:
        return await self._session.scalar(get_one_by_id_query, {"id": id_})
