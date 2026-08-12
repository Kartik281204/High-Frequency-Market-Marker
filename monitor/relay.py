#!/usr/bin/env python3
"""
Bridges the Rust engine's plain TCP snapshot stream (newline-delimited JSON,
127.0.0.1:7878) to a WebSocket the dashboard can connect to from a browser
(127.0.0.1:8765), and forwards a "KILL" message from the dashboard back to
the engine's control channel on the same TCP connection.

Deliberately a separate process from the engine: this is exactly the kind of
thing you keep off the trading hot path in a real system. If this script
lags, hangs, or gets GC-paused, it has zero effect on the matching engine or
the strategy -- they don't know or care whether anything is even connected.
"""

import asyncio
import json
import sys

import websockets

ENGINE_HOST = "127.0.0.1"
ENGINE_PORT = 7878
WS_HOST = "127.0.0.1"
WS_PORT = 8765
RECONNECT_DELAY_SECS = 1.0

dashboard_clients: set[websockets.ServerConnection] = set()
engine_writer: asyncio.StreamWriter | None = None
latest_snapshot: str | None = None


async def broadcast(message: str) -> None:
    if not dashboard_clients:
        return
    stale = []
    for ws in dashboard_clients:
        try:
            await ws.send(message)
        except websockets.ConnectionClosed:
            stale.append(ws)
    for ws in stale:
        dashboard_clients.discard(ws)


async def engine_reader_loop() -> None:
    """Maintains a connection to the engine, forwarding every snapshot line
    to all connected dashboards. Reconnects automatically if the engine
    isn't up yet or restarts."""
    global engine_writer, latest_snapshot
    while True:
        try:
            reader, writer = await asyncio.open_connection(ENGINE_HOST, ENGINE_PORT)
            engine_writer = writer
            print(f"[relay] connected to engine at {ENGINE_HOST}:{ENGINE_PORT}", file=sys.stderr)
            while True:
                line = await reader.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                latest_snapshot = text
                await broadcast(text)
        except (ConnectionRefusedError, OSError) as e:
            print(f"[relay] engine not reachable yet ({e}); retrying in {RECONNECT_DELAY_SECS}s", file=sys.stderr)
        finally:
            engine_writer = None
        await asyncio.sleep(RECONNECT_DELAY_SECS)


async def handle_dashboard(ws: "websockets.ServerConnection") -> None:
    dashboard_clients.add(ws)
    print(f"[relay] dashboard connected ({len(dashboard_clients)} total)", file=sys.stderr)
    try:
        if latest_snapshot is not None:
            await ws.send(latest_snapshot)
        async for message in ws:
            if isinstance(message, bytes):
                continue
            if message.strip().upper() == "KILL":
                if engine_writer is not None:
                    engine_writer.write(b"KILL\n")
                    await engine_writer.drain()
                    print("[relay] forwarded KILL to engine", file=sys.stderr)
                else:
                    print("[relay] KILL requested but engine is not connected", file=sys.stderr)
    except websockets.ConnectionClosed:
        pass
    finally:
        dashboard_clients.discard(ws)
        print(f"[relay] dashboard disconnected ({len(dashboard_clients)} total)", file=sys.stderr)


async def main() -> None:
    asyncio.create_task(engine_reader_loop())
    async with websockets.serve(handle_dashboard, WS_HOST, WS_PORT):
        print(f"[relay] websocket server on ws://{WS_HOST}:{WS_PORT}", file=sys.stderr)
        print("[relay] open monitor/dashboard.html in a browser now", file=sys.stderr)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
