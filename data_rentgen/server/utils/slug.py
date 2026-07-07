# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


def slugify(text: str) -> str:
    """Convert `Some value` to `some-value`.

    Used to convert `FastAPI.title` to short name which can be used as label for Prometheus metrics.
    """
    return text.lower().strip().replace(" ", "-")
