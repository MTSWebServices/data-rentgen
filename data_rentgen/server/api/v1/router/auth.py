# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response
from fastapi.security import OAuth2PasswordRequestForm

from data_rentgen.db.models.user import User
from data_rentgen.dependencies import Stub
from data_rentgen.server.errors.registration import get_error_responses
from data_rentgen.server.errors.schemas.invalid_request import InvalidRequestSchema
from data_rentgen.server.errors.schemas.logout import LogoutErrorSchema
from data_rentgen.server.errors.schemas.not_authorized import (
    NotAuthorizedRedirectSchema,
    NotAuthorizedSchema,
)
from data_rentgen.server.errors.schemas.not_implemented import NotImplementedErrorSchema
from data_rentgen.server.providers.auth import AuthProvider
from data_rentgen.server.schemas.v1.auth import AuthTokenSchema
from data_rentgen.server.services import PersonalTokenPolicy, get_user

router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
)


@router.post(
    "/token",
    summary="Get auth token",
    responses=get_error_responses(
        include={
            NotAuthorizedSchema,
            NotAuthorizedRedirectSchema,
            InvalidRequestSchema,
            NotImplementedErrorSchema,
        },
    ),
)
async def token(
    auth_provider: Annotated[AuthProvider, Depends(Stub(AuthProvider))],
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> AuthTokenSchema:
    user_token = await auth_provider.get_token_password_grant(
        login=form_data.username,
        password=form_data.password,
    )
    return AuthTokenSchema.model_validate(user_token)


@router.get(
    "/callback",
    summary="Handle redirect callback from OAuth2 provider",
    responses=get_error_responses(
        include={
            NotAuthorizedSchema,
            InvalidRequestSchema,
            NotImplementedErrorSchema,
        },
    ),
    status_code=HTTPStatus.NO_CONTENT,
)
async def auth_callback(
    request: Request,
    code: str,
    auth_provider: Annotated[AuthProvider, Depends(Stub(AuthProvider))],
):
    code_grant = await auth_provider.get_token_authorization_code_grant(
        code=code,
    )
    request.session["access_token"] = code_grant["access_token"]
    request.session["refresh_token"] = code_grant["refresh_token"]
    return Response(status_code=HTTPStatus.NO_CONTENT)


@router.get(
    "/logout",
    summary="Logout user",
    responses=get_error_responses(
        include={
            NotAuthorizedSchema,
            InvalidRequestSchema,
            LogoutErrorSchema,
            NotImplementedErrorSchema,
        },
    ),
    status_code=HTTPStatus.NO_CONTENT,
)
async def logout(
    request: Request,
    current_user: Annotated[User, Depends(get_user(personal_token_policy=PersonalTokenPolicy.DENY))],
    auth_provider: Annotated[AuthProvider, Depends(Stub(AuthProvider))],
):
    await auth_provider.logout(user=current_user, request=request)
    return Response(status_code=HTTPStatus.NO_CONTENT)
