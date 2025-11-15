"""
WebSocket connection manager for real-time chat
"""

from fastapi import WebSocket
from typing import List, Dict
import logging
import json

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str = None):
        """Accept and register new WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)

        if user_id:
            if user_id not in self.user_connections:
                self.user_connections[user_id] = []
            self.user_connections[user_id].append(websocket)

        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket, user_id: str = None):
        """Remove WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

        if user_id and user_id in self.user_connections:
            if websocket in self.user_connections[user_id]:
                self.user_connections[user_id].remove(websocket)
            if not self.user_connections[user_id]:
                del self.user_connections[user_id]

        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send message to specific WebSocket"""
        await websocket.send_text(message)

    async def send_personal_json(self, data: dict, websocket: WebSocket):
        """Send JSON to specific WebSocket"""
        await websocket.send_json(data)

    async def broadcast(self, message: str):
        """Broadcast message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")

    async def broadcast_json(self, data: dict):
        """Broadcast JSON to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except Exception as e:
                logger.error(f"Error broadcasting JSON to connection: {e}")

    async def send_to_user(self, user_id: str, message: str):
        """Send message to all connections of a specific user"""
        if user_id in self.user_connections:
            for connection in self.user_connections[user_id]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.error(f"Error sending to user {user_id}: {e}")

    def get_connection_count(self) -> int:
        """Get total active connections"""
        return len(self.active_connections)

    def get_user_connection_count(self, user_id: str) -> int:
        """Get connection count for specific user"""
        return len(self.user_connections.get(user_id, []))
