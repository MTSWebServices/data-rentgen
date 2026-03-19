# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from sqlalchemy import BigInteger, Index, String, column, func
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class User(Base):
    __tablename__ = "user"
    __table_args__ = (Index("ix__user__name_lower", func.lower(column("name")), unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
