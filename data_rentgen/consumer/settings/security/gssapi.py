# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import os
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import Any, Literal

import anyio
from faststream.security import SASLGSSAPI
from pydantic import Field, FilePath, SecretStr, model_validator

from data_rentgen.consumer.settings.security.base import KafkaSecurityBaseSettings

logger = logging.getLogger(__name__)


class KafkaSecurityGSSAPISettings(KafkaSecurityBaseSettings):
    """Kafka GSSAPI auth settings.

    This auth method requires installing `data-rentgen[gssapi]` extra,
    and relies on presence of `kinit` and `kdestroy` binaries in your operating system.

    Examples
    --------

    Using principal + password for calling `kinit`:

    ```bash
    DATA_RENTGEN__KAFKA__SECURITY__TYPE=GSSAPI
    DATA_RENTGEN__KAFKA__SECURITY__PRINCIPAL=dummy
    DATA_RENTGEN__KAFKA__SECURITY__PASSWORD=changeme
    DATA_RENTGEN__KAFKA__SECURITY__REALM=MY.REALM.COM
    ```
    Using principal + keytab for calling `kinit`:

    ```bash
    DATA_RENTGEN__KAFKA__SECURITY__TYPE=GSSAPI
    DATA_RENTGEN__KAFKA__SECURITY__PRINCIPAL=dummy
    DATA_RENTGEN__KAFKA__SECURITY__KEYTAB=/etc/security/dummy.keytab
    DATA_RENTGEN__KAFKA__SECURITY__REALM=MY.REALM.COM
    ```
    """

    type: Literal["GSSAPI"] = "GSSAPI"
    principal: str = Field(description="Kerberos principal")
    password: SecretStr | None = Field(description="Kerberos password. Mutually exclusive with keytab")
    keytab: FilePath | None = Field(default=None, description="Path to keytab file. Mutually exclusive with password")
    realm: str | None = Field(default=None, description="Kerberos realm")
    service_name: str = Field(default="kafka", description="Kerberos service name")
    refresh_interval_seconds: int = Field(default=60 * 60, description="Refresh Kerberos ticket every N seconds")

    @model_validator(mode="after")
    def _check_password_or_keytab(self):
        if not (self.password or self.keytab):
            msg = "input should contain either 'password' or 'keytab' field, both are empty"
            raise ValueError(msg)
        if self.password and self.keytab:
            msg = "input should contain either 'password' or 'keytab' field, both are set"
            raise ValueError(msg)
        return self

    def to_security(self):
        return SASLGSSAPI()

    def extra_broker_kwargs(self) -> dict[str, Any]:
        return {
            "sasl_kerberos_service_name": self.service_name,
            "sasl_kerberos_domain_name": self.realm,
        }

    async def kinit_password(self):
        cmd = ["kinit", self.principal]
        logger.debug("Calling command: %s", " ".join(cmd))

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            # do not show user 'Please enter password' banner
            stdout=asyncio.subprocess.PIPE,
            # do not capture stderr, immediately show all errors to user
        )
        password: str = self.password.get_secret_value()  # type: ignore[assignment]
        stdout, stderr = await process.communicate(password.encode("utf-8"))
        retcode = await process.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd, stdout, stderr)

    async def kinit_keytab(self):
        keytab: Path = self.keytab.resolve()  # type: ignore[assignment]
        cmd = ["kinit", self.principal, "-k", "-t", os.fspath(keytab)]
        logger.debug("Calling command: %s", " ".join(cmd))

        process = await asyncio.create_subprocess_exec(*cmd)
        retcode = await process.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd)

    async def kinit_refresh(self):
        cmd = ["kinit", "-R", self.principal]
        logger.debug("Calling command: %s", " ".join(cmd))

        process = await asyncio.create_subprocess_exec(*cmd)
        retcode = await process.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd)

    async def kdestroy(self):
        cmd = ["kdestroy", "-p", self.principal]
        logger.debug("Calling command: %s", " ".join(cmd))

        process = await asyncio.create_subprocess_exec(*cmd)
        retcode = await process.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, cmd)

    async def initialize(self) -> None:
        if self.keytab:
            await self.kinit_keytab()
        elif self.password:
            await self.kinit_password()

    async def refresh(self) -> None:
        while True:
            await anyio.sleep(self.refresh_interval_seconds)

            with suppress(Exception):
                # if ticket is renewable, try to refresh it
                await self.kinit_refresh()
                continue

            await self.initialize()

    async def destroy(self) -> None:
        with suppress(Exception):
            await self.kdestroy()
