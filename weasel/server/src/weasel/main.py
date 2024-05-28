import hashlib
import json
import signal
import sys
import time
from datetime import datetime

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
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
        while is_connected:
            data = await websocket.receive_text()
            message = json.loads(data)
            logger.info("Received message", extra={"ws_msg": message})

            with get_session() as session:
                await handle_message(message, chunks, websocket, session)
    except WebSocketDisconnect:
        is_connected = False
        if websocket in clients:
            clients[websocket]["disconnected"] = True
        logger.info("Client disconnected.")
    finally:
        if is_connected:
            await websocket.close()


async def handle_message(message, chunks, websocket, session):
    start_time = time.time()
    message_type = message.get("type")
    require_receipt = message.get("requireReceipt", True)

    if not message_type:
        logger.warning("Received message without type", extra={"ws_msg": message})
        return

    handler_mapping = {"chunk": handle_chunk, "full_message": handle_full_message, "log": handle_log}

    if handler := handler_mapping.get(message_type):
        await handler(message, chunks, websocket, session, require_receipt)
    else:
        logger.warning("Unknown message type", extra={"ws_msg_type": message_type})

    end_time = time.time()
    logger.info(
        "Processing time for message type",
        extra={"ws_msg_type": message_type, "duration": end_time - start_time},
    )


async def handle_chunk(message, chunks, websocket, session, require_receipt):
    chunk, error_message = parse_message(Chunk, message)
    if error_message:
        await send_error_response(websocket, "chunk", error_message)
        return

    client_chunks = chunks.setdefault(chunk.clientId, [])
    if not validate_checksum(chunk):
        await send_error_response(websocket, "chunk", "Checksum mismatch")
        return

    client_chunks.append(chunk.chunk)
    if len(client_chunks) == chunk.total:
        await process_full_message(chunks, chunk.clientId, websocket, session, require_receipt)

    if require_receipt:
        await send_success_response(websocket, "chunk", "Chunk received successfully")


async def handle_full_message(message, chunks, websocket, session, require_receipt):  # noqa: ARG001
    full_message, error_message = parse_message(FullMessage, message)
    if error_message:
        await send_error_response(websocket, "full_message", error_message)
        return

    stored_message = session.exec(select(Message).where(Message.hash == full_message.hash)).first()
    if not require_receipt:
        return

    if stored_message:
        await send_success_response(
            websocket, "full_message", "Message already received", hash=full_message.hash
        )
    else:
        await send_error_response(websocket, "full_message", "Message not found", hash=full_message.hash)


async def handle_log(message, chunks, websocket, session, require_receipt):  # noqa: ARG001
    payload = message.get("payload")
    if not payload:
        logger.error("Log message missing payload")
        return

    log_message, error_message = parse_message(LogMessageSchema, payload)
    if error_message:
        logger.exception("Invalid log message data")
        return

    log_entry = LogMessage(
        level=log_message.level,
        message=log_message.message,
        timestamp=datetime.fromisoformat(log_message.timestamp.replace("Z", "+00:00")),
    )
    session.add(log_entry)
    session.commit()
    logger.info("Log received", extra={"ws_msg": log_message.message})

    if require_receipt:
        await send_success_response(websocket, "log", "Log received successfully")


async def process_full_message(chunks, client_id, websocket, session, require_receipt):
    full_message_str = "".join(chunks[client_id])
    full_message_hash = hashlib.sha256(full_message_str.encode()).hexdigest()
    full_message = FullMessage(type="full_message", hash=full_message_hash, content=full_message_str)

    stored_message = Message(content=full_message.content, hash=full_message.hash)
    session.add(stored_message)
    session.commit()
    chunks[client_id] = []

    if require_receipt:
        await send_success_response(websocket, "full_message", "Message received", hash=full_message_hash)


def parse_message(schema, message):
    try:
        return schema(**message), None
    except ValidationError as ve:
        logger.exception("Validation error while parsing message", exc_info=ve.errors())
        return None, f"Validation error: {ve}"
    except TypeError as te:
        # This can occur if there are incorrect arguments to the schema or bad data types
        logger.exception("Type error in message parsing", exc_info=te)
        return None, f"Type error: {te}"


async def send_error_response(websocket, message_type, error_message, **extra_fields):
    response = {
        "type": message_type,
        "payload": {"status": "error", "message": error_message, **extra_fields},
    }
    await websocket.send_text(json.dumps(response))


async def send_success_response(websocket, message_type, success_message, **extra_fields):
    response = {
        "type": message_type,
        "payload": {"status": "success", "message": success_message, **extra_fields},
    }
    await websocket.send_text(json.dumps(response))


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
