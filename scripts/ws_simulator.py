"""Simple WebSocket simulator to reproduce handshake + immediate close behavior.

Run this locally and point `SYSTEM_WS_URL` to ws://localhost:8765 to test the client.
"""
import asyncio
import json
import logging
import os
import signal

import websockets

LOG = logging.getLogger("ws_simulator")


async def handler(ws, path):
    try:
        LOG.info("Client connected path=%s", path)
        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
        LOG.info("Received handshake: %s", raw)
        # Reply with handshake-ack then close after short delay
        ack = {"type": "handshake-ack", "payload": {"status": "ok", "ack_type": "handshake"}}
        await ws.send(json.dumps(ack, separators=(",", ":")))
        LOG.info("Sent handshake-ack, will close connection in 0.2s to reproduce issue")
        await asyncio.sleep(0.2)
        await ws.close(code=4000, reason="simulated-close")
        LOG.info("Closed connection with code=4000 simulated-close")

    except Exception as e:
        LOG.exception("Simulator handler error: %s", e)


def main():
    logging.basicConfig(level=logging.INFO)
    port = int(os.getenv("WS_SIMULATOR_PORT", "9876"))
    stop = asyncio.Event()

    async def _run():
        server = await websockets.serve(handler, "0.0.0.0", port)
        LOG.info("WebSocket simulator listening on ws://0.0.0.0:%d", port)

        await stop.wait()
        server.close()
        await server.wait_closed()

    def _signal(_sig, _frame):
        LOG.info("Signal received, stopping simulator")
        stop.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _signal)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
