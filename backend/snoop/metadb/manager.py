import mitmproxy.flow
from sqlmodel import Session, SQLModel, create_engine, select

from snoop.common import Singleton
from snoop.common.config import DATABASE_URL
from snoop.metadb.models import URL, HTTPExchange, HTTPMessage


class MetaDB(metaclass=Singleton):
    def __init__(self, url=None):
        self.db_url = url or DATABASE_URL
        self.engine = create_engine(self.db_url, echo=True)
        SQLModel.metadata.create_all(self.engine)

    def store(self, exchange: mitmproxy.flow.Flow) -> None:
        with Session(self.engine) as session:
            url = URL.from_string(exchange.request.url)
            session.add(url)
            session.commit()

            request = HTTPMessage.from_mitm(url, exchange.request)
            response = HTTPMessage.from_mitm(url, exchange.response)
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

    def retrieve(self, url: str) -> None:
        with Session(self.engine) as session:
            statement = select(HTTPExchange).where(HTTPExchange.url.raw == url)
            return session.exec(statement).first()

    def close(self):
        self.engine.dispose()
        type(self).clear()
