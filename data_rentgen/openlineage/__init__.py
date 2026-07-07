# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""OpenLineage models & facets.

Currently, openlineage-python [does not support](https://github.com/OpenLineage/OpenLineage/issues/2629) deserialization from JSON.
So we have to write our own deserialization logic.

Also FastStream support only `pydantic` models whether openlineage-python provides `attrs` models.
"""  # noqa: E501
