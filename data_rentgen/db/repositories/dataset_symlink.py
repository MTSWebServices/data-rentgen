# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection

from sqlalchemy import any_, bindparam, or_, select
from sqlalchemy.dialects.postgresql import insert

from data_rentgen.db.models.dataset_symlink import DatasetSymlink, DatasetSymlinkType
from data_rentgen.db.models.dataset_symlink_group import DatasetSymlinkGroup
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetSymlinkGroupDTO

insert_group_query = insert(DatasetSymlinkGroup).on_conflict_do_nothing(
    index_elements=[DatasetSymlinkGroup.dataset_id, DatasetSymlinkGroup.fingerprint],
)

get_list_query = select(DatasetSymlink).where(
    or_(
        DatasetSymlink.from_dataset_id == any_(bindparam("dataset_ids")),
        DatasetSymlink.to_dataset_id == any_(bindparam("dataset_ids")),
    ),
)


class DatasetSymlinkRepository(Repository[DatasetSymlinkGroup]):
    async def create_bulk(self, items: list[DatasetSymlinkGroupDTO]):
        if not items:
            return

        await self._session.execute(
            insert_group_query,
            [
                {
                    "fingerprint": item.fingerprint,
                    "dataset_id": dataset.id,
                    "type": DatasetSymlinkType(type_),
                }
                for item in items
                for dataset, type_ in item.members
            ],
        )

    async def list_by_dataset_ids(self, dataset_ids: Collection[int]) -> list[DatasetSymlink]:
        if not dataset_ids:
            return []

        scalars = await self._session.scalars(get_list_query, {"dataset_ids": list(dataset_ids)})
        return list(scalars.all())
