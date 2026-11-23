from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class AlertNotifier:
    """Sends real-time alerts to Bark / Feishu / Slack / Generic Webhook + local logs."""

    def __init__(self, webhook_url: Optional[str] = None) -> None:
        self.webhook_url = webhook_url or os.getenv("ZY_ALERT_WEBHOOK_URL", "")

    def send(self, title: str, message: str, level: str = "INFO") -> None:
        logger.info("[%s] %s — %s", level, title, message)
        if not self.webhook_url:
            return
        try:
            payload = {
                "msg_type": "text",
                "title": f"[zy-quant][{level}] {title}",
                "text": f"[zy-quant][{level}] {title}\n{message}",
                "content": {"text": f"[zy-quant][{level}] {title}\n{message}"},
            }
            with httpx.Client(timeout=5.0) as client:
                client.post(self.webhook_url, json=payload)
        except Exception as exc:
            logger.warning("Webhook notification failed: %s", exc)
