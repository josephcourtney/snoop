from pydantic import BaseModel


class Chunk(BaseModel):
    type: str
    index: int
    total: int
    chunk: str
    checksum: int
    clientId: str  # noqa: N815 # this mirrors a javascript key name


class FullMessage(BaseModel):
    type: str
    hash: str
    content: str


class LogMessage(BaseModel):
    level: str
    message: str
    timestamp: str
