# SPDX-FileCopyrightText: 2026-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

import os

from pydantic_settings import BaseSettings as PydanticBaseSettings
from pydantic_settings import (
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class BaseSettings(PydanticBaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DATA_RENTGEN__",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[PydanticBaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_file_path = os.getenv("DATA_RENTGEN_CONFIG_FILE", "config.yml")
        yaml_settings = YamlConfigSettingsSource(
            settings_cls,
            yaml_file=yaml_file_path,
            yaml_file_encoding="utf-8",
        )
        return (
            init_settings,
            yaml_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
