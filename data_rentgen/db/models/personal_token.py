# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import UUID as SQL_UUID
from sqlalchemy import BigInteger, Column, Date, DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB, ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_rentgen.db.models.base import Base
from data_rentgen.db.models.user import User


class PersonalToken(Base):
    __tablename__ = "personal_token"
    __table_args__ = (
        ExcludeConstraint(
            (Column("user_id"), "="),
            # Postgres converts daterange[since, until] to daterange[since, until + 1 day),
            # which is fine for DB queries but not for returning values to user.
            # So we store columns separately.
            (func.daterange(Column("since"), Column("until"), "[]"), "&&"),
            # not all queries use WHERE name = .. filter,
            # so this column should be at the end
            (Column("name"), "="),
            where=Column("revoked_at").is_(None),
            using="gist",
            name="uq__personal_token__user_id_since_until_name",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        SQL_UUID,
        primary_key=True,
        doc="Unique token ID. Generated as UUIDv7",
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
        doc="Id of corresponding user",
    )
    user: Mapped[User] = relationship(User, lazy="noload")
    name: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        doc="Token name",
    )
    scopes: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        doc="Token scopes",
    )
    since: Mapped[date] = mapped_column(
        Date(),
        nullable=False,
        doc="Day then token is valid from",
    )
    until: Mapped[date] = mapped_column(
        Date(),
        nullable=False,
        doc="Day when token is valid to (inclusive)",
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when user token was revoked, or None if not",
    )
