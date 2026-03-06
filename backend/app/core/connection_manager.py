import asyncio
import json
from typing import Dict, List, Set

from fastapi import WebSocket


class ConnectionManager:
    """Manages WebSocket connections grouped by run_id for real-time pipeline updates."""

    def __init__(self):
        self._connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, run_id: str):
        await websocket.accept()
        async with self._lock:
            if run_id not in self._connections:
                self._connections[run_id] = set()
            self._connections[run_id].add(websocket)

    async def disconnect(self, websocket: WebSocket, run_id: str):
        async with self._lock:
            if run_id in self._connections:
                self._connections[run_id].discard(websocket)
                if not self._connections[run_id]:
                    del self._connections[run_id]

    async def broadcast(self, run_id: str, message: dict):
        """Broadcast a JSON message to all subscribers of a run_id."""
        payload = json.dumps(message)
        dead: List[WebSocket] = []

        async with self._lock:
            targets = set(self._connections.get(run_id, set()))

        for ws in targets:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)

        for ws in dead:
            await self.disconnect(ws, run_id)

    def active_connections_count(self) -> int:
        return sum(len(v) for v in self._connections.values())


# Singleton instance shared across the app
manager = ConnectionManager()
