# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import sys
import zlib
from collections.abc import Callable
from http import HTTPStatus

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

# https://stackoverflow.com/a/22311297/23601543
GZIP = 16 | zlib.MAX_WBITS

# Cap decompressed body size to 256 MiB
MAX_DECOMPRESSED_SIZE = 256 * 1024 * 1024

if sys.version_info >= (3, 15):
    # https://docs.python.org/3.15/whatsnew/3.15.html#whatsnew315-bytearray-take-bytes
    def join_bytes(inp: bytearray) -> bytes:
        return inp.take_bytes()
else:

    def join_bytes(inp: bytearray) -> bytes:
        return bytes(inp)


def get_chunk_size(decompressed_chunk: bytes, total_decompressed: int):
    chunk_size = len(decompressed_chunk)
    if total_decompressed + chunk_size > MAX_DECOMPRESSED_SIZE:
        msg = f"Request body is too large: {total_decompressed} bytes"
        raise HTTPException(status_code=HTTPStatus.REQUEST_ENTITY_TOO_LARGE, detail=msg)
    return chunk_size


class SupportsGzipRequest(Request):
    # https://fastapi.tiangolo.com/how-to/custom-request-and-route/#create-a-custom-gziprequest-class
    async def body(self) -> bytes:
        if hasattr(self, "_body"):
            return self._body

        content_encoding = self.headers.getlist("Content-Encoding")
        if "gzip" in content_encoding:
            decompressor = zlib.decompressobj(GZIP)
        elif content_encoding:
            msg = f"Unsupported Content-Encoding: {content_encoding}"
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=msg)
        else:
            return await super().body()

        total_decompressed = 0
        decompressed: bytearray = bytearray()
        async for compressed_chunk in self.stream():
            chunk = decompressor.decompress(compressed_chunk)
            total_decompressed += get_chunk_size(chunk, total_decompressed)
            decompressed += chunk

        chunk = decompressor.flush()
        total_decompressed += get_chunk_size(chunk, total_decompressed)
        decompressed += chunk
        self._body = join_bytes(decompressed)
        return self._body


class SupportsGzipRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            request = SupportsGzipRequest(request.scope, request.receive)
            return await original_route_handler(request)

        return custom_route_handler
