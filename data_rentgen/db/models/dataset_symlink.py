# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from enum import Enum

from sqlalchemy import BigInteger, ForeignKey, MetaData, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy_utils import ChoiceType

_view_metadata = MetaData()


class _ViewBase(DeclarativeBase):
    metadata = _view_metadata


class DatasetSymlinkType(str, Enum):
    METASTORE = "METASTORE"
    WAREHOUSE = "WAREHOUSE"

    def __str__(self) -> str:
        return self.value


class DatasetSymlink(_ViewBase):
    """Read-only ORM mapping for the dataset_symlink VIEW."""

    __tablename__ = "dataset_symlink"

    from_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    to_dataset_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dataset.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    type: Mapped[DatasetSymlinkType] = mapped_column(
        ChoiceType(DatasetSymlinkType, impl=String(32)),
        nullable=False,
    )
