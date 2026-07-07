# SPDX-FileCopyrightText: 2023 Andrey Tikhonov (@Tishka17)
# SPDX-License-Identifier: Apache-2.0


# Implementation copied from:
# https://github.com/Tishka17/deseos17/blob/master/src/deseos17/presentation/web_api/dependencies/depends_stub.py
class Stub:
    """
    This class is used to prevent fastapi from digging into
    real dependencies attributes detecting them as request data

    So instead of
    `value: Annotated[MyDependency, Depends()]`
    Write
    `value: Annotated[MyDependency, Depends(Stub(MyDependency))]`

    And then you can declare how to create it:
    `app.dependency_overrides[MyDependency] = my_dependency_factory`

    """

    def __init__(self, dependency: type, **kwargs):
        self._dependency = dependency
        self._kwargs = kwargs

    def __call__(self):
        raise NotImplementedError

    def __eq__(self, other) -> bool:
        if isinstance(other, Stub):
            return self._dependency == other._dependency and self._kwargs == other._kwargs
        if not self._kwargs:
            return self._dependency == other
        return False

    def __hash__(self):
        if not self._kwargs:
            return hash(self._dependency)
        serial = (
            self._dependency,
            *self._kwargs.items(),
        )
        return hash(serial)

    def __repr__(self):
        args = "".join(f", {k}={v!r}" for k, v in self._kwargs.items())
        class_name = self.__class__.__name__
        dependency_class = self._dependency.__name__
        return f"{class_name}({dependency_class}{args})"
