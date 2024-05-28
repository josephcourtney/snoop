import sqlalchemy.engine
from sqlmodel import Field, Session, SQLModel, select

from snoop.doop.compressor import Compressor

from .abc import KeyValueStore


class KeyValue(SQLModel, table=True):
    key: bytes = Field(primary_key=True)
    value: bytes


class SQLiteKeyValueStore(KeyValueStore):
    def __init__(self, engine: sqlalchemy.engine.Engine, compressor: Compressor | None = None):
        super().__init__(compressor)
        self.engine = engine
        SQLModel.metadata.create_all(self.engine)

    def _put(self, key: str, value: bytes) -> None:
        with Session(self.engine) as session:
            if existing_entry := session.exec(select(KeyValue).where(KeyValue.key == key)).first():
                existing_entry.value = value
            else:
                session.add(KeyValue(key=key, value=value))
            session.commit()

    def _get(self, key: str) -> bytes | None:
        with Session(self.engine) as session:
            entry = session.exec(select(KeyValue).where(KeyValue.key == key)).first()
            return entry.value if entry else None

    def _delete(self, key: str) -> None:
        with Session(self.engine) as session:
            if entry := session.exec(select(KeyValue).where(KeyValue.key == key)).first():
                session.delete(entry)
                session.commit()

    def _put_batch(self, items: list[tuple[str, bytes]]) -> None:
        with Session(self.engine) as session:
            for key, value in items:
                if existing_entry := session.exec(select(KeyValue).where(KeyValue.key == key)).first():
                    existing_entry.value = value
                else:
                    session.add(KeyValue(key=key, value=value))
            session.commit()

    def _get_batch(self, keys: list[str]) -> list[bytes | None]:
        with Session(self.engine) as session:
            result = []
            for key in keys:
                entry = session.exec(select(KeyValue).where(KeyValue.key == key)).first()
                result.append(entry.value if entry else None)
            return result

    def _delete_batch(self, keys: list[str]) -> None:
        with Session(self.engine) as session:
            for key in keys:
                if entry := session.exec(select(KeyValue).where(KeyValue.key == key)).first():
                    session.delete(entry)
            session.commit()
