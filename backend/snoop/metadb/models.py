import datetime
import enum
from pathlib import PurePosixPath
from typing import Union
from urllib.parse import parse_qsl, urlparse

import mitmproxy.http
import tzlocal
from pydantic import ConfigDict
from sqlmodel import JSON, Field, SQLModel


class URL(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int | None = Field(default=None, primary_key=True)
    raw: str
    scheme: str
    host: str
    username: str | None
    password: str | None
    port: int | None
    path: str | None
    query: dict[str, list[str]] | None = Field(sa_type=JSON)
    fragment: str | None

    @classmethod
    def from_string(cls, url_str: str) -> Union["URL", None]:
        parsed_url = urlparse(url_str)
        query = parse_qsl(parsed_url.query, keep_blank_values=True)
        return cls(
            raw=url_str,
            scheme=parsed_url.scheme,
            host=parsed_url.hostname,
            username=parsed_url.username,
            password=parsed_url.password,
            port=parsed_url.port,
            path=str(PurePosixPath(parsed_url.path)),
            query=query,
            fragment=parsed_url.fragment,
        )


class MessageType(enum.Enum):
    HTTP_REQUEST = enum.auto()
    HTTP_RESPONSE = enum.auto()


class HTTPMessage(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int | None = Field(default=None, primary_key=True)
    http_version: str
    message_type: MessageType

    url_id: int | None = Field(default=None, foreign_key="url.id")
    headers: dict[str, list[str]] | None = Field(sa_type=JSON)
    content: bytes
    trailers: dict[str, list[str]] | None = Field(sa_type=JSON)
    time_start: datetime.datetime
    time_end: datetime.datetime | None

    # request-specific
    method: str | None

    # response-specific
    status_code: int | None
    reason: str | None

    @classmethod
    def from_mitm(cls, url: URL, mitm_message: mitmproxy.http.Message) -> Union["HTTPMessage", None]:
        if mitm_message is None:
            return None
        message_trailers = {}
        if mitm_message.trailers:
            for k, v in mitm_message.trailers.fields:
                message_trailers[k.decode("utf-8")] = [
                    *message_trailers.get(k.decode("utf-8"), []),
                    v.decode("utf-8"),
                ]
        message_headers = {}
        if mitm_message.headers:
            for k, v in mitm_message.headers.fields:
                message_headers[k.decode("utf-8")] = [
                    *message_headers.get(k.decode("utf-8"), []),
                    v.decode("utf-8"),
                ]

        message_time_start = (
            None
            if mitm_message.timestamp_start is None
            else datetime.datetime.fromtimestamp(mitm_message.timestamp_start, tz=tzlocal.get_localzone())
        )
        message_time_end = (
            None
            if mitm_message.timestamp_end is None
            else datetime.datetime.fromtimestamp(mitm_message.timestamp_end, tz=tzlocal.get_localzone())
        )
        if isinstance(mitm_message, mitmproxy.http.Request):
            message_type = MessageType.HTTP_REQUEST
            method = mitm_message.method
            status_code = None
            reason = None
        elif isinstance(mitm_message, mitmproxy.http.Response):
            message_type = MessageType.HTTP_RESPONSE
            method = None
            status_code = mitm_message.status_code
            reason = mitm_message.reason
        else:
            message_type = None
            method = None
            status_code = None
            reason = None

        return cls(
            http_version=mitm_message.http_version,
            message_type=message_type,
            url_id=url.id,
            headers=message_headers,
            content=mitm_message.content,
            trailers=message_trailers,
            time_start=message_time_start,
            time_end=message_time_end,
            method=method,
            status_code=status_code,
            reason=reason,
        )


class HTTPExchange(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    request_id: int | None = Field(default=None, foreign_key="httpmessage.id")
    response_id: int | None = Field(default=None, foreign_key="httpmessage.id")
    type: str
