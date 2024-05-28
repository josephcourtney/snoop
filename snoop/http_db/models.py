from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlmodel import Field, Relationship, SQLModel, column_property, create_engine


# http metadata
class URL(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    scheme: str
    host: str
    username: str | None
    password: str | None
    port: int | None
    path: str | None
    raw_query: str | None
    fragment: str | None
    raw: str | None = column_property(
        func.concat(
            func.coalesce("scheme" + "://", ""),
            func.coalesce("host", ""),
            func.coalesce("/" + "path", ""),
            func.coalesce("?" + "raw_query", ""),
            func.coalesce("#" + "fragment", ""),
        )
    )
    query_params: list["QueryParam"] = Relationship(back_populates="url")
    http_messages: list["HTTPMessage"] = Relationship(back_populates="url")


class QueryParam(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    url_id: int = Field(foreign_key="url.id")
    name: str
    value: str
    url: URL = Relationship(back_populates="query_params")


class HTTPMessage(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    http_version: str
    message_type: str
    url_id: int | None = Field(foreign_key="url.id")
    content_id: bytes  # identifier used to retrieve content from blob store
    time_start: datetime
    time_end: datetime | None
    method: str | None
    status_code_id: int | None = Field(foreign_key="statuscode.id")
    url: URL | None = Relationship(back_populates="http_messages")
    headers: list["Header"] = Relationship(back_populates="httpmessage")
    trailers: list["Trailer"] = Relationship(back_populates="httpmessage")
    status_code: Optional["StatusCode"] = Relationship(back_populates="http_messages")


class StatusCode(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    status_code: int
    reason: str
    http_messages: list[HTTPMessage] = Relationship(back_populates="status_code")


class Header(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    httpmessage_id: int = Field(foreign_key="httpmessage.id")
    name: str
    value: str
    httpmessage: HTTPMessage = Relationship(back_populates="headers")


class Trailer(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    httpmessage_id: int = Field(foreign_key="httpmessage.id")
    name: str
    value: str
    httpmessage: HTTPMessage = Relationship(back_populates="trailers")


class HTTPExchange(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    request_id: int | None = Field(foreign_key="httpmessage.id")
    response_id: int | None = Field(foreign_key="httpmessage.id")
    type: str


# Create database engine
engine = create_engine("sqlite:///database.db")

# Create tables
SQLModel.metadata.create_all(engine)
