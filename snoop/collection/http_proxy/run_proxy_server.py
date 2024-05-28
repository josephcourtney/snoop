import asyncio
from pathlib import Path

from mitmproxy.tools.main import mitmdump

from snoop.metadb.manager import MetaDB
from snoop.proxy_server.websocket_server import WebsocketServer


def main():
    capture_path = Path(__file__).parent / "capture.py"
    MetaDB()  # Initialize the database
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(WebsocketServer())
        mitmdump(["-s", str(capture_path)])  # Start mitmdump with the capture script
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
