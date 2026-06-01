from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError


@dataclass(slots=True)
class TelegramNotifier:
    bot_token: str | None
    chat_id: str | None
    logger: logging.Logger | None = None
    timeout_seconds: float = 10.0

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send(self, message: str) -> bool:
        if not self.enabled:
            return False
        return await asyncio.to_thread(self._send_sync, message)

    def notify(self, message: str) -> asyncio.Task[bool] | None:
        if not self.enabled:
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        return loop.create_task(self.send(message))

    def _send_sync(self, message: str) -> bool:
        logger = self.logger or logging.getLogger(__name__)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": message[:4096],
            "disable_web_page_preview": True,
        }
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        req = request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                return 200 <= resp.status < 300
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            logger.warning("Telegram notification failed: %s", exc)
            return False


def build_executor_alert(title: str, **fields: Any) -> str:
    lines = [f"[Marcus Local Executor] {title}"]
    for key, value in fields.items():
        if value in (None, "", []):
            continue
        label = key.replace("_", " ")
        lines.append(f"{label}: {value}")
    return "\n".join(lines)
