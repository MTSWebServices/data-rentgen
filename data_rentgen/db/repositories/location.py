# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection

from sqlalchemy import (
    ColumnElement,
    CompoundSelect,
    Select,
    SQLColumnExpression,
    any_,
    asc,
    bindparam,
    desc,
    func,
    select,
    union,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Location
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import LocationDTO, PaginationDTO
from data_rentgen.exceptions.entity import EntityNotFoundError

get_one_by_name_query = select(Location).where(
    Location.type == bindparam("type"),
    Location.name == bindparam("name"),
)
get_one_by_addresses_query = (
    select(Location)
    .join(Location.addresses)
    .where(
        Location.type == bindparam("type"),
        Address.url == any_(bindparam("addresses")),
    )
)
get_one_query = (
    select(Location)
    .from_statement(
        get_one_by_name_query.union(get_one_by_addresses_query),
    )
    .options(selectinload(Location.addresses))
)
get_distinct_query = select(Location.type).distinct(Location.type).order_by(Location.type)

insert_address_query = (
    insert(Address)
    .values(
        {
            "location_id": bindparam("location_id"),
            "url": bindparam("url"),
        }
    )
    .on_conflict_do_nothing(index_elements=["location_id", "url"])
)


class LocationRepository(Repository[Location]):
    async def paginate(
        self,
        page: int,
        page_size: int,
        location_ids: Collection[int],
        location_type: Collection[str],
        search_query: str | None,
    ) -> PaginationDTO[Location]:
        where = []
        if location_ids:
            where.append(Location.id == any_(list(location_ids)))  # type: ignore[arg-type]
        if location_type:
            location_type_lower = [location_type.lower() for location_type in location_type]
            where.append(Location.type == any_(location_type_lower))  # type: ignore[arg-type]

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            location_stmt = select(Location, ts_rank(Location.search_vector, tsquery).label("search_rank")).where(
                ts_match(Location.search_vector, tsquery),
                *where,
            )
            address_stmt = (
                select(Location, func.max(ts_rank(Address.search_vector, tsquery)).label("search_rank"))
                .join(Location, Address.location_id == Location.id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Location.id, Address.id)
            )

            union_cte = union(location_stmt, address_stmt).cte()

            location_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *location_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*location_columns)
            order_by = [desc("search_rank"), asc("name"), asc("type")]
        else:
            query = select(Location).where(*where)
            order_by = [Location.name, Location.type]

        options = [selectinload(Location.addresses)]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def update_external_id(self, location_id: int, external_id: str | None) -> Location:
        query = select(Location).where(Location.id == location_id).options(selectinload(Location.addresses))
        location = await self._session.scalar(query)
        if not location:
            raise EntityNotFoundError("Location", "id", location_id)  # noqa: EM101
        location.external_id = external_id
        await self._session.flush([location])
        return location

    async def create_or_update(self, location: LocationDTO) -> Location:
        result = await self._get(location)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(location.type, location.name)
            result = await self._get(location)

        if not result:
            result = await self._create(location)

        await self._update_addresses(result, location)
        return result

    async def _get(self, location: LocationDTO) -> Location | None:
        return await self._session.scalar(
            get_one_query,
            {
                "type": location.type,
                "name": location.name,
                "addresses": list(location.addresses),
            },
        )

    async def _create(self, location: LocationDTO) -> Location:
        result = Location(type=location.type, name=location.name)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def _update_addresses(self, existing: Location, new: LocationDTO) -> Location:
        existing_urls = {address.url for address in existing.addresses}
        new_urls = new.addresses - existing_urls
        # in most cases, Location is unchanged, so we can avoid INSERT statements
        if not new_urls:
            return existing

        # take a lock to avoid creating the same address from multiple workers
        await self._lock(existing.type, existing.name)
        await self._session.execute(
            insert_address_query,
            [
                {
                    "location_id": existing.id,
                    "url": url,
                }
                for url in new_urls
            ],
        )
        return existing

    async def get_location_types(self):
        scalars = await self._session.scalars(get_distinct_query)
        return scalars.all()
