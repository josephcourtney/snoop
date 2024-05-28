import contextlib
import datetime
import enum
import hashlib
from pathlib import PurePosixPath
from typing import Union
from urllib.parse import parse_qsl, urlparse

import mitmproxy.flow
import mitmproxy.http
import tzlocal
from pydantic import ConfigDict
from sqlmodel import JSON, Field, Session, SQLModel, create_engine, select

from snoop.common import Singleton
from snoop.common.config import DATABASE_URL
from snoop.doop import BlobExistsError, BlobStore, DummyCompressor, FixedSizeChunker, SQLiteKeyValueStore


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
    content_hash: str
    trailers: dict[str, list[str]] | None = Field(sa_type=JSON)
    time_start: datetime.datetime
    time_end: datetime.datetime | None

    # request-specific
    method: str | None

    # response-specific
    status_code: int | None
    reason: str | None


class HTTPExchange(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    request_id: int | None = Field(default=None, foreign_key="httpmessage.id")
    response_id: int | None = Field(default=None, foreign_key="httpmessage.id")
    type: str


class MetaDB(metaclass=Singleton):
    def __init__(self, url=None):
        self.db_url = url or DATABASE_URL
        self.engine = create_engine(self.db_url, echo=True)

        # Initialize BlobStore
        self.blob_store = BlobStore(
            db_engine=self.engine,
            chunker=FixedSizeChunker(chunk_size=1024),
            kv_store=SQLiteKeyValueStore(self.engine, compressor=DummyCompressor()),
        )

        SQLModel.metadata.create_all(self.engine)

    def store(self, exchange: mitmproxy.flow.Flow) -> None:
        with Session(self.engine) as session:
            url = URL.from_string(exchange.request.url)
            session.add(url)
            session.commit()

            request = self.build_message(url, exchange.request)
            response = self.build_message(url, exchange.response)
            session.add(request)
            session.add(response)
            session.commit()

            if exchange.error:
                error_message = exchange.error
                error_time = exchange.error
            else:
                error_message = None
                error_time = None

            exchange = HTTPExchange(
                request_id=None if request is None else request.id,
                response_id=None if response is None else response.id,
                error_message=error_message,
                error_time=error_time,
                type=exchange.type,
            )
            session.add(exchange)
            session.commit()

    def build_message(self, url: URL, mitm_message: mitmproxy.http.Message) -> HTTPMessage | None:
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

        return HTTPMessage(
            http_version=mitm_message.http_version,
            message_type=message_type,
            url_id=url.id,
            headers=message_headers,
            content_hash=self.store_content(mitm_message.content),
            trailers=message_trailers,
            time_start=message_time_start,
            time_end=message_time_end,
            method=method,
            status_code=status_code,
            reason=reason,
        )

    def store_content(self, content_data: bytes) -> str:
        content_hash = hashlib.sha256(content_data).hexdigest()
        with contextlib.suppress(BlobExistsError):
            self.blob_store.store_blob(identifier=content_hash, blob_data=content_data)
        print(f"CHUNKS: {len(self.blob_store.kv_store.store)}")
        return content_hash

    def retrieve(self, url: str) -> None:
        with Session(self.engine) as session:
            statement = select(HTTPExchange).where(HTTPExchange.url.raw == url)
            return session.exec(statement).first()

    def close(self):
        self.engine.dispose()
        type(self).clear()
