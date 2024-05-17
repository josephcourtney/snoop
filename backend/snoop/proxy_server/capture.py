import dotenv
import mitmproxy.http

from snoop.metadb.manager import MetaDB

config = dotenv.dotenv_values()


class Capture:
    def __init__(self):
        self.metadb = MetaDB()

    def response(self, exchange: mitmproxy.http.HTTPFlow) -> None:
        if exchange.response:
            exchange.response.headers.get("Content-Type")
        self.metadb.store(exchange)


addons = [Capture()]
