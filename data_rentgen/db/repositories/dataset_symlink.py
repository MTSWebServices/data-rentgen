# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Collection
from uuid import UUID

from sqlalchemy import ARRAY, BigInteger, and_, any_, bindparam, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased

from data_rentgen.db.models.dataset_symlink import DatasetSymlinkType
from data_rentgen.db.models.dataset_symlink_group import DatasetSymlinkGroup
from data_rentgen.db.repositories.base import Repository
from data_rentgen.dto import DatasetSymlinkGroupDTO

get_fingerprints_query = (
    select(DatasetSymlinkGroup.fingerprint.distinct())
    .where(
        DatasetSymlinkGroup.fingerprint == any_(bindparam("fingerprints")),
    )
    .limit(bindparam("limit"))
)

insert_group_query = insert(DatasetSymlinkGroup).on_conflict_do_nothing(
    index_elements=[DatasetSymlinkGroup.dataset_id, DatasetSymlinkGroup.fingerprint],
)

group_member = aliased(DatasetSymlinkGroup, name="group_member")
neighbour_group_member = aliased(DatasetSymlinkGroup, name="neighbour_group_member")

closure_base_part = select(
    func.unnest(bindparam("dataset_ids", type_=ARRAY(BigInteger()))).label("dataset_id"),
)
closure_cte = closure_base_part.cte("reachable_datasets", recursive=True)
closure_recursive_part = (
    select(neighbour_group_member.dataset_id.label("dataset_id"))
    .select_from(closure_cte)
    .join(group_member, group_member.dataset_id == closure_cte.c.dataset_id)
    .join(
        neighbour_group_member,
        and_(
            neighbour_group_member.fingerprint == group_member.fingerprint,
            neighbour_group_member.dataset_id != closure_cte.c.dataset_id,
        ),
    )
)
closure_cte = closure_cte.union(closure_recursive_part)

get_symlink_groups_query = select(DatasetSymlinkGroup).join(
    closure_cte,
    DatasetSymlinkGroup.dataset_id == closure_cte.c.dataset_id,
)


class DatasetSymlinkRepository(Repository[DatasetSymlinkGroup]):
    async def create_bulk(self, items: list[DatasetSymlinkGroupDTO]):
        if not items:
            return

        # skip inserting existing symlink groups
        missing_fingerprints = await self._get_missing_fingerprints({item.fingerprint for item in items})
        to_insert = [item for item in items if item.fingerprint in missing_fingerprints]
        if not to_insert:
            return

        await self._session.execute(
            insert_group_query,
            [
                {
                    "fingerprint": item.fingerprint,
                    "dataset_id": dataset.id,
                    "type": DatasetSymlinkType(type_),
                }
                for item in to_insert
                for dataset, type_ in item.members
            ],
        )

    async def _get_missing_fingerprints(self, fingerprints: set[UUID]) -> set[UUID]:
        existing = await self._session.scalars(
            get_fingerprints_query,
            {"fingerprints": list(fingerprints), "limit": len(fingerprints)},
        )
        return fingerprints - set(existing.all())

    async def get_symlink_groups(self, dataset_ids: Collection[int]) -> list[DatasetSymlinkGroup]:
        if not dataset_ids:
            return []

        scalars = await self._session.scalars(
            get_symlink_groups_query,
            {"dataset_ids": list(dataset_ids)},
        )
        return list(scalars.all())
