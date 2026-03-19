# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from uuid import UUID

from sqlalchemy import JSON, BigInteger
from sqlalchemy import UUID as SQL_UUID
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class Schema(Base):
    __tablename__ = "schema"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    digest: Mapped[UUID] = mapped_column(
        SQL_UUID,
        nullable=False,
        index=True,
        unique=True,
        doc="Schema SHA-1 digest based on fields content. Currently this is in form of UUID",
    )
    fields: Mapped[dict] = mapped_column(
        JSON,
        nullable=False,
        doc="Schema fields",
    )
