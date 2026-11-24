from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional


class RingBufferLogHandler(logging.Handler):
    """
    Thread-safe structured backend log ring buffer for real-time Web Console streaming.
    Captures Python logging records across gateway, daemon, strategy, and risk modules.
    """

    def __init__(self, capacity: int = 300) -> None:
        super().__init__()
        self._buffer: Deque[Dict[str, Any]] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._seq: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            short_logger = record.name.replace("zy_quant.", "")
            with self._lock:
                self._seq += 1
                self._buffer.append(
                    {
                        "id": self._seq,
                        "timestamp": ts,
                        "level": record.levelname,
                        "module": short_logger,
                        "message": msg,
                    }
                )
        except Exception:
            self.handleError(record)

    def append_custom(self, level: str, module: str, message: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self._seq += 1
            self._buffer.append(
                {
                    "id": self._seq,
                    "timestamp": ts,
                    "level": level.upper(),
                    "module": module,
                    "message": message,
                }
            )

    def query(
        self,
        since_id: int = 0,
        level: Optional[str] = None,
        limit: int = 150,
    ) -> List[Dict[str, Any]]:
        lvl = level.upper() if level and level.upper() != "ALL" else None
        with self._lock:
            items = [
                entry
                for entry in self._buffer
                if entry["id"] > since_id and (lvl is None or entry["level"] == lvl)
            ]
        return items[-limit:]
