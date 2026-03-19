# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from enum import Enum

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.dataset import Dataset


class DatasetSymlinkType(str, Enum):
    METASTORE = "METASTORE"
    WAREHOUSE = "WAREHOUSE"

    def __str__(self) -> str:
        return self.value


class DatasetSymlink(Base):
    __tablename__ = "dataset_symlink"
    __table_args__ = (UniqueConstraint("from_dataset_id", "to_dataset_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    from_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    from_dataset: Mapped[Dataset] = relationship(
        Dataset,
        lazy="noload",
        foreign_keys=[from_dataset_id],
    )

    to_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    to_dataset: Mapped[Dataset] = relationship(
        Dataset,
        lazy="noload",
        foreign_keys=[to_dataset_id],
    )

    type: Mapped[DatasetSymlinkType] = mapped_column(
        ChoiceType(DatasetSymlinkType, impl=String(32)),
        nullable=False,
        doc="Type of dataset symlink, e.g. metastore table -> hdfs location",
    )
