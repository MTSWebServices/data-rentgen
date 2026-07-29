# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date  # noqa: TC003
from http import HTTPStatus
from typing import Annotated
from uuid import UUID  # noqa: TC003

from fastapi import APIRouter, Depends, Response
from pydantic import ValidationError
from pydantic_core import InitErrorDetails

from data_rentgen.db.models import User  # noqa: TC001
from data_rentgen.server.errors import get_error_responses
from data_rentgen.server.providers.auth.personal_token_provider import PersonalTokenAuthProvider  # noqa: TC001
from data_rentgen.server.schemas.v1 import (
    PageResponseV1,
    PersonalTokenCreatedDetailedResponseV1,
    PersonalTokenCreateRequestV1,
    PersonalTokenDetailedResponseV1,
    PersonalTokenPaginateQueryV1,
    PersonalTokenResetRequestV1,
    PersonalTokenResponseV1,
    PersonalTokenScopeV1,
)
from data_rentgen.server.services import (
    PersonalTokenPolicy,
    PersonalTokenService,
    get_personal_token_provider,
    get_user,
)

router = APIRouter(
    prefix="/personal-tokens",
    tags=["Personal Tokens"],
    responses=get_error_responses(),
)


@router.get("")
async def get_personal_tokens(
    current_user: Annotated[User, Depends(get_user())],
    user_token_service: Annotated[PersonalTokenService, Depends()],
    query_args: Annotated[PersonalTokenPaginateQueryV1, Depends()],
) -> PageResponseV1[PersonalTokenDetailedResponseV1]:
    pagination = await user_token_service.paginate(
        page=query_args.page,
        page_size=query_args.page_size,
        user=current_user,
        personal_token_ids=query_args.personal_token_id,
    )
    return PageResponseV1[PersonalTokenDetailedResponseV1].from_pagination(pagination)


@router.post("")
async def create_personal_token(
    token_params: PersonalTokenCreateRequestV1,
    current_user: Annotated[User, Depends(get_user(personal_token_policy=PersonalTokenPolicy.DENY))],
    user_token_service: Annotated[PersonalTokenService, Depends()],
    personal_token_auth_provider: Annotated[PersonalTokenAuthProvider, Depends(get_personal_token_provider)],
) -> PersonalTokenCreatedDetailedResponseV1:
    async with user_token_service:
        token = await user_token_service.create(
            user=current_user,
            name=token_params.name,
            # TODO: user-defined scope list
            scopes=[PersonalTokenScopeV1.ALL_READ, PersonalTokenScopeV1.ALL_WRITE],
            until=token_params.until,
        )
        validate_until(token_params.until, token.until)

    return PersonalTokenCreatedDetailedResponseV1(
        id=token.id,
        data=PersonalTokenResponseV1(
            id=token.id,
            name=token.name,
            scopes=[PersonalTokenScopeV1(scope) for scope in token.scopes],
            since=token.since,
            until=token.until,
        ),
        content=personal_token_auth_provider.generate_jwt(user=current_user, token=token),
    )


@router.patch("/{token_id}")
async def reset_personal_token(
    token_id: UUID,
    new_token_params: PersonalTokenResetRequestV1,
    current_user: Annotated[User, Depends(get_user(personal_token_policy=PersonalTokenPolicy.DENY))],
    user_token_service: Annotated[PersonalTokenService, Depends()],
    personal_token_auth_provider: Annotated[PersonalTokenAuthProvider, Depends(get_personal_token_provider)],
) -> PersonalTokenCreatedDetailedResponseV1:
    async with user_token_service:
        old_token = await user_token_service.revoke(current_user, token_id)
        new_token = await user_token_service.create(
            user=current_user,
            name=old_token.name,
            scopes=old_token.scopes,
            until=new_token_params.until,
        )
        validate_until(new_token_params.until, new_token.until)

    return PersonalTokenCreatedDetailedResponseV1(
        id=new_token.id,
        data=PersonalTokenResponseV1(
            id=new_token.id,
            name=new_token.name,
            scopes=[PersonalTokenScopeV1(scope) for scope in new_token.scopes],
            since=new_token.since,
            until=new_token.until,
        ),
        content=personal_token_auth_provider.generate_jwt(user=current_user, token=new_token),
    )


@router.delete("/{token_id}", status_code=HTTPStatus.NO_CONTENT)
async def revoke_personal_token(
    token_id: UUID,
    current_user: Annotated[User, Depends(get_user(personal_token_policy=PersonalTokenPolicy.DENY))],
    user_token_service: Annotated[PersonalTokenService, Depends()],
):
    async with user_token_service:
        await user_token_service.revoke(current_user, token_id)
    return Response(status_code=HTTPStatus.NO_CONTENT)


def validate_until(input_until: date | None, actual_until: date) -> None:
    if not input_until:
        return

    if input_until > actual_until:
        raise ValidationError.from_exception_data(
            title=f"Input should be less than or equal to {actual_until.isoformat()}",
            line_errors=[
                InitErrorDetails(
                    type="less_than_equal",
                    loc=("body", "until"),
                    input=input_until,
                    ctx={"le": actual_until.isoformat()},
                ),
            ],
            input_type="json",
        )
