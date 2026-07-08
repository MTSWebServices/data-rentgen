# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, Text
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class SQLQuery(Base):
    __tablename__ = "sql_query"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fingerprint: Mapped[UUID] = mapped_column(
        SQL_UUID,
        index=True,
        unique=True,
        nullable=False,
        doc="Fingerprint to identify sql query",
    )
    query: Mapped[str] = mapped_column(Text, nullable=False, doc="SQL query")
