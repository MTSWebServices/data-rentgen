# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, text
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE, ExcludeConstraint, Range
from sqlalchemy.orm import Mapped, mapped_column

from data_rentgen.db.models.base import Base


class CustomUserProperties(Base):
    __tablename__ = "custom_user_properties"
    __table_args__ = (
        ExcludeConstraint(
            ("user_id", "="),
            ("property_id", "="),
            ("during", "&&"),
            name="uq__custom_user_properties__user_id_property_id_during",
            using="gist",
        ),
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("user.id", ondelete="CASCADE"),
        nullable=False,
        doc="Id of corresponding user",
    )

    property_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("custom_properties.id", ondelete="CASCADE"),
        nullable=False,
        doc="Id of user's custom property",
    )

    value: Mapped[JSONB] = mapped_column(
        JSONB,
        nullable=False,
        doc="Value of user's custom property",
    )

    during: Mapped[Range[datetime]] = mapped_column(
        TSTZRANGE,
        server_default=text("tstzrange(now(), NULL, '[)')"),
        nullable=False,
        doc="Duration when value of user's custom property is valid",
    )
