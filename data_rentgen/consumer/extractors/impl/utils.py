# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
def parse_kv_tag(key: str, default_source: str | None = None):
    key, value, *rest = key.split(":")
    source = rest[0] if rest else default_source
    return key, value, source
