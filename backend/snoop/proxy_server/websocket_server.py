import asyncio
from typing import ClassVar

import websockets

from snoop.common import AsyncInit


class WebsocketServer(metaclass=AsyncInit):
    connected_clients: ClassVar[set[websockets.WebSocketServerProtocol]] = set()

    async def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.server = await websockets.serve(self._websocket_handler, self.host, self.port)

    async def _websocket_handler(self, websocket: websockets.WebSocketServerProtocol, path: str) -> None:  # noqa: ARG002 # externally defined API
        await self.register_client(websocket)

    async def register_client(self, websocket: websockets.WebSocketServerProtocol) -> None:
        self.connected_clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            self.connected_clients.remove(websocket)

    async def notify_clients(self, message: str) -> None:
        if self.connected_clients:
            await asyncio.gather(*(client.send(message) for client in self.connected_clients))

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()
