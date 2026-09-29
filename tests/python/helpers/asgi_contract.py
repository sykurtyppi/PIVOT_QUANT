from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode


@dataclass
class AsgiResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body.decode("utf-8"))

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")


async def _invoke(
    app: Any,
    method: str,
    path: str,
    query_string: str,
    body: bytes,
    headers: list[tuple[bytes, bytes]],
) -> AsgiResponse:
    sent_request = False
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal sent_request
        if sent_request:
            return {"type": "http.disconnect"}
        sent_request = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query_string.encode("utf-8"),
        "headers": headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "root_path": "",
    }

    await app(scope, receive, send)

    response_start = next(msg for msg in messages if msg["type"] == "http.response.start")
    status_code = int(response_start["status"])
    response_headers = {
        key.decode("latin1").lower(): value.decode("latin1")
        for key, value in response_start.get("headers", [])
    }
    body_chunks = [msg.get("body", b"") for msg in messages if msg["type"] == "http.response.body"]
    response_body = b"".join(body_chunks)
    return AsgiResponse(status_code=status_code, headers=response_headers, body=response_body)


def request_json(
    app: Any,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> AsgiResponse:
    query_string = urlencode(params or {}, doseq=True)
    body = b""
    headers: list[tuple[bytes, bytes]] = [(b"host", b"testserver")]
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        headers.extend(
            [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ]
        )
    return asyncio.run(_invoke(app, method, path, query_string, body, headers))

