import datetime

from sqlmodel import Field, SQLModel


class Message(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    content: str
    received_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    hash: str


class LogMessage(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    level: str
    message: str
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
