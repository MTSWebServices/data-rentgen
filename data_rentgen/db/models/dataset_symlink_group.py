# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.dataset import Dataset
from data_rentgen.db.models.dataset_symlink import DatasetSymlinkType


class DatasetSymlinkGroup(Base):
    __tablename__ = "dataset_symlink_group"

    dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    dataset: Mapped[Dataset] = relationship(
        Dataset,
        lazy="noload",
        foreign_keys=[dataset_id],
    )
    fingerprint: Mapped[UUID] = mapped_column(SQL_UUID, primary_key=True, index=True)
    type: Mapped[DatasetSymlinkType] = mapped_column(
        ChoiceType(DatasetSymlinkType, impl=String(32)),
        nullable=False,
    )
