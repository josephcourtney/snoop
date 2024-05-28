import hashlib
import json
import signal
import sys
import time
from datetime import datetime

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlmodel import select

from .database import get_session, init_db
from .logging import get_logger
from .models import LogMessage, Message
from .schemas import Chunk, FullMessage
from .schemas import LogMessage as LogMessageSchema

logger = get_logger()

app = FastAPI()


init_db()

clients = {}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    is_connected = True
    chunks = {}

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            logger.info("Received message", extra={"ws_msg": message})

            with get_session() as session:
                await handle_message(message, chunks, websocket, session, is_connected)
    except WebSocketDisconnect:
        is_connected = False
        if websocket in clients:
            clients[websocket]["disconnected"] = True
        logger.info("Client disconnected.")
    finally:
        if is_connected:
            await websocket.close()


async def handle_message(message, chunks, websocket, session, is_connected):
    start_time = time.time()
    message_type = message.get("type")
    require_receipt = message.get("requireReceipt", True)
    if not message_type:
        logger.warning("Received message without type", extra={"ws_msg": message})
        return

    if message_type == "chunk":
        await handle_chunk(message, chunks, websocket, session, is_connected, require_receipt)
    elif message_type == "full_message":
        await handle_full_message(message, websocket, session, is_connected, require_receipt)
    elif message_type == "log":
        await handle_log(message, websocket, session, is_connected, require_receipt)
    else:
        logger.warning("Unknown message type", extra={"ws_msg_type": message_type})
        return
    end_time = time.time()
    logger.info(
        "Processing time for message type",
        extra={"ws_msg_type": message_type, "duration": end_time - start_time},
    )


async def handle_chunk(message, chunks, websocket, session, is_connected, require_receipt):
    try:
        chunk = Chunk(**message)
    except Exception as e:
        if not is_connected:
            return
        await websocket.send_text(
            json.dumps({
                "type": "chunk",
                "payload": {"status": "error", "message": f"Invalid chunk data: {e}"},
            })
        )
        logger.exception("Invalid chunk data")
        return

    if chunk.clientId not in chunks:
        chunks[chunk.clientId] = []

    if not validate_checksum(chunk):
        if not is_connected:
            return
        await websocket.send_text(
            json.dumps({"type": "chunk", "payload": {"status": "error", "message": "Checksum mismatch"}})
        )
        logger.error("Checksum mismatch")
        return

    chunks[chunk.clientId].append(chunk.chunk)
    if len(chunks[chunk.clientId]) == chunk.total:
        await process_full_message(chunks, chunk.clientId, websocket, session, is_connected, require_receipt)

    if is_connected and require_receipt:
        await websocket.send_text(
            json.dumps({
                "type": "chunk",
                "payload": {"status": "success", "message": "Chunk received successfully"},
            })
        )


async def handle_full_message(message, websocket, session, is_connected, require_receipt):
    try:
        full_message = FullMessage(**message)
    except Exception as e:
        if not is_connected:
            return
        await websocket.send_text(
            json.dumps({
                "type": "full_message",
                "payload": {"status": "error", "message": f"Invalid full message data: {e}"},
            })
        )
        logger.exception("Invalid full message data")
        return

    stored_message = session.exec(select(Message).where(Message.hash == full_message.hash)).first()
    if not is_connected or not require_receipt:
        return
    if stored_message:
        await websocket.send_text(
            json.dumps({
                "type": "full_message",
                "payload": {
                    "status": "success",
                    "message": "Message already received",
                    "hash": full_message.hash,
                },
            })
        )
    else:
        await websocket.send_text(
            json.dumps({
                "type": "full_message",
                "payload": {
                    "status": "error",
                    "message": "Message not found",
                    "hash": full_message.hash,
                },
            })
        )


async def handle_log(message, websocket, session, is_connected, require_receipt):
    payload = message.get("payload")
    if not payload:
        logger.error("Log message missing payload")
        return

    try:
        log_message = LogMessageSchema(**payload)
    except Exception:
        logger.exception("Invalid log message data")
        return  # Avoid sending error responses to prevent infinite loop

    log_entry = LogMessage(
        level=log_message.level,
        message=log_message.message,
        timestamp=datetime.fromisoformat(log_message.timestamp.replace("Z", "+00:00")),
    )
    session.add(log_entry)
    session.commit()
    logger.info("Log received", extra={"ws_msg": log_message.message})

    if is_connected and require_receipt:
        await websocket.send_text(
            json.dumps({
                "type": "log",
                "payload": {"status": "success", "message": "Log received successfully"},
            })
        )


async def process_full_message(chunks, client_id, websocket, session, is_connected, require_receipt):
    full_message_str = "".join(chunks[client_id])
    full_message_hash = hashlib.sha256(full_message_str.encode()).hexdigest()
    full_message = FullMessage(type="full_message", hash=full_message_hash, content=full_message_str)

    stored_message = Message(content=full_message.content, hash=full_message.hash)
    session.add(stored_message)
    session.commit()

    if not is_connected:
        return
    if require_receipt:
        await websocket.send_text(
            json.dumps({
                "type": "full_message",
                "payload": {
                    "status": "success",
                    "message": "Message received",
                    "hash": full_message_hash,
                },
            })
        )
    chunks[client_id] = []


def validate_checksum(chunk: Chunk) -> bool:
    calculated_checksum = sum(ord(char) for char in chunk.chunk)
    return calculated_checksum == chunk.checksum


def handle_exit(signal, frame):  # noqa: ARG001
    logger.info("Shutting down gracefully...")
    for client in clients:
        client.close()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


def main():
    uvicorn.run(app, host="127.0.0.1", port=8767)


if __name__ == "__main__":
    main()
