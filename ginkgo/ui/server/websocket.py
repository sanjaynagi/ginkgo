"""WebSocket framing helpers for the UI server."""

from __future__ import annotations

import base64
import hashlib
import json
from struct import pack
from typing import Any

WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def websocket_accept(key: str) -> str:
    """Return the WebSocket accept header for a client key.

    Parameters
    ----------
    key : str
        Client-provided WebSocket key.

    Returns
    -------
    str
        Base64-encoded WebSocket accept value.
    """
    digest = hashlib.sha1(f"{key}{WEBSOCKET_GUID}".encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def send_websocket_json(connection: Any, payload: dict[str, Any]) -> None:
    """Send one JSON text frame over a WebSocket connection.

    Parameters
    ----------
    connection : Any
        Active socket-like connection.
    payload : dict[str, Any]
        JSON-serializable payload.

    Returns
    -------
    None
        This method writes directly to the connection.
    """
    send_websocket_frame(connection, json.dumps(payload, sort_keys=True).encode("utf-8"))


def send_websocket_frame(connection: Any, payload: bytes) -> None:
    """Send one unfragmented text frame.

    Parameters
    ----------
    connection : Any
        Active socket-like connection.
    payload : bytes
        Encoded text frame payload.

    Returns
    -------
    None
        This method writes directly to the connection.
    """
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(pack("!H", length))
    else:
        header.append(127)
        header.extend(pack("!Q", length))
    connection.sendall(bytes(header) + payload)
