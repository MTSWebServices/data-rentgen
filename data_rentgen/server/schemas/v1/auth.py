# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel


class AuthTokenSchema(BaseModel):
    access_token: str
    token_type: str
    expires_at: float
