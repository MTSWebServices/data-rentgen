# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from fastapi import Request

from data_rentgen.server.providers.auth.base_provider import AuthProvider


async def get_auth_provider(request: Request) -> AuthProvider:
    return request.app.state.auth_provider


async def get_personal_token_provider(request: Request) -> AuthProvider:
    return request.app.state.personal_token_auth_provider
