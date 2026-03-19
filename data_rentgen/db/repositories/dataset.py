# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from collections.abc import Collection

from sqlalchemy import (
    ARRAY,
    ColumnElement,
    CompoundSelect,
    Integer,
    Row,
    Select,
    SQLColumnExpression,
    String,
    any_,
    asc,
    bindparam,
    cast,
    desc,
    distinct,
    func,
    select,
    tuple_,
    union,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from data_rentgen.db.models import Address, Dataset, Location, TagValue
from data_rentgen.db.models.dataset import DatasetTagValue
from data_rentgen.db.repositories.base import Repository
from data_rentgen.db.utils.search import make_tsquery, ts_match, ts_rank
from data_rentgen.dto import DatasetDTO, PaginationDTO

fetch_bulk_query = select(Dataset).where(
    tuple_(Dataset.location_id, func.lower(Dataset.name)).in_(
        select(
            func.unnest(
                cast(bindparam("location_ids"), ARRAY(Integer())),
                cast(bindparam("names_lower"), ARRAY(String())),
            )
            .table_valued("location_id", "name_lower")
            .render_derived(),
        ),
    ),
)

get_list_query = (
    select(Dataset)
    .where(Dataset.id == any_(bindparam("dataset_ids")))
    .options(selectinload(Dataset.location).selectinload(Location.addresses))
    .options(selectinload(Dataset.tag_values).selectinload(TagValue.tag))
)

get_one_query = select(Dataset).where(
    Dataset.location_id == bindparam("location_id"),
    func.lower(Dataset.name) == bindparam("name_lower"),
)

get_stats_query = (
    select(
        Dataset.location_id.label("location_id"),
        func.count(Dataset.id.distinct()).label("total_datasets"),
    )
    .where(
        Dataset.location_id == any_(bindparam("location_ids")),
    )
    .group_by(Dataset.location_id)
)

insert_tag_value_query = (
    insert(DatasetTagValue)
    .values(
        {
            "dataset_id": bindparam("dataset_id"),
            "tag_value_id": bindparam("tag_value_id"),
        }
    )
    .on_conflict_do_nothing(index_elements=["dataset_id", "tag_value_id"])
)


class DatasetRepository(Repository[Dataset]):
    async def fetch_bulk(self, datasets_dto: list[DatasetDTO]) -> list[tuple[DatasetDTO, Dataset | None]]:
        if not datasets_dto:
            return []

        scalars = await self._session.scalars(
            fetch_bulk_query,
            {
                "location_ids": [item.location.id for item in datasets_dto],
                "names_lower": [item.name.lower() for item in datasets_dto],
            },
        )
        existing = {(dataset.location_id, dataset.name.lower()): dataset for dataset in scalars.all()}
        return [
            (
                dto,
                existing.get((dto.location.id, dto.name.lower())),  # type: ignore[arg-type]
            )
            for dto in datasets_dto
        ]

    async def create_or_update(self, dataset: DatasetDTO) -> Dataset:
        result = await self._get(dataset)
        if not result:
            # try one more time, but with lock acquired.
            # if another worker already created the same row, just use it. if not - create with holding the lock.
            await self._lock(dataset.location.id, dataset.name.lower())
            result = await self._get(dataset)

        if not result:
            result = await self._create(dataset)
        return await self.update(result, dataset)

    async def _get(self, dataset: DatasetDTO) -> Dataset | None:
        return await self._session.scalar(
            get_one_query,
            {
                "location_id": dataset.location.id,
                "name_lower": dataset.name.lower(),
            },
        )

    async def _create(self, dataset: DatasetDTO) -> Dataset:
        result = Dataset(location_id=dataset.location.id, name=dataset.name)
        self._session.add(result)
        await self._session.flush([result])
        return result

    async def update(self, existing: Dataset, new: DatasetDTO) -> Dataset:
        if not new.tag_values:
            # in most cases datasets have no tag values, so we can avoid INSERT statements
            return existing

        # Lock to prevent inserting the same rows from multiple workers
        await self._lock(existing.location_id, existing.name)
        await self._session.execute(
            insert_tag_value_query,
            [
                {
                    "dataset_id": existing.id,
                    "tag_value_id": tag_value_dto.id,
                }
                for tag_value_dto in new.tag_values
            ],
        )
        # unlike job tags, OL integrations may provide only part of dataset tags.
        # so we only insert them, but never delete old ones
        return existing

    async def paginate(
        self,
        page: int,
        page_size: int,
        dataset_ids: Collection[int],
        tag_value_ids: Collection[int],
        location_ids: Collection[int],
        location_types: Collection[str],
        search_query: str | None,
    ) -> PaginationDTO[Dataset]:
        where = []
        location_join_clause = Location.id == Dataset.location_id
        if dataset_ids:
            where.append(Dataset.id == any_(list(dataset_ids)))  # type: ignore[arg-type]
        if location_ids:
            where.append(Dataset.location_id == any_(list(location_ids)))  # type: ignore[arg-type]
        if location_types:
            location_type_lower = [location_type.lower() for location_type in location_types]
            where.append(Location.type == any_(location_type_lower))  # type: ignore[arg-type]

        if tag_value_ids:
            tv_ids = list(tag_value_ids)
            dataset_ids_subq = (
                select(Dataset.id)
                .join(Dataset.tag_values)
                .where(TagValue.id.in_(tv_ids))
                .group_by(Dataset.id)
                # If multiple tag values are passed, dataset should have both of them (AND, not OR)
                .having(func.count(distinct(TagValue.id)) == len(tv_ids))
            )
            where.append(Dataset.id.in_(dataset_ids_subq))

        query: Select | CompoundSelect
        order_by: list[ColumnElement | SQLColumnExpression]
        if search_query:
            tsquery = make_tsquery(search_query)

            dataset_stmt = (
                select(Dataset, ts_rank(Dataset.search_vector, tsquery).label("search_rank"))
                .join(Location, location_join_clause)
                .where(ts_match(Dataset.search_vector, tsquery), *where)
            )
            location_stmt = (
                select(Dataset, ts_rank(Location.search_vector, tsquery).label("search_rank"))
                .join(Location, location_join_clause)
                .where(ts_match(Location.search_vector, tsquery), *where)
            )
            address_stmt = (
                select(Dataset, func.max(ts_rank(Address.search_vector, tsquery)).label("search_rank"))
                .join(Location, location_join_clause)
                .join(Address, Address.location_id == Dataset.location_id)
                .where(ts_match(Address.search_vector, tsquery), *where)
                .group_by(Dataset.id, Location.id, Address.id)
            )

            union_cte = union(dataset_stmt, location_stmt, address_stmt).cte()

            dataset_columns = [column for column in union_cte.columns if column.name != "search_rank"]

            query = select(
                *dataset_columns,
                func.max(union_cte.c.search_rank).label("search_rank"),
            ).group_by(*dataset_columns)
            order_by = [desc("search_rank"), asc("name")]
        else:
            query = select(Dataset).join(Location, location_join_clause).where(*where)
            order_by = [Dataset.name]

        options = [
            selectinload(Dataset.location).selectinload(Location.addresses),
            selectinload(Dataset.tag_values).selectinload(TagValue.tag),
        ]
        return await self._paginate_by_query(
            query=query,
            order_by=order_by,
            options=options,
            page=page,
            page_size=page_size,
        )

    async def list_by_ids(self, dataset_ids: Collection[int]) -> list[Dataset]:
        if not dataset_ids:
            return []
        result = await self._session.scalars(get_list_query, {"dataset_ids": list(dataset_ids)})
        return list(result.all())

    async def get_stats_by_location_ids(self, location_ids: Collection[int]) -> dict[int, Row]:
        if not location_ids:
            return {}

        query_result = await self._session.execute(get_stats_query, {"location_ids": list(location_ids)})
        return {row.location_id: row for row in query_result.all()}
