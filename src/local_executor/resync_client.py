from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from typing import Any, Callable
from urllib import error, parse, request


@dataclass(slots=True)
class PositionSyncClient:
    base_url: str
    token: str
    timeout_seconds: float
    ssl_verify: bool = True
    urlopen_func: Callable[..., Any] = request.urlopen

    def fetch_current_position(self, bot_id: str) -> dict[str, Any]:
        encoded_bot_id = parse.quote(bot_id, safe="")
        url = f"{self.base_url}/api/v1/bots/{encoded_bot_id}/current-position"

        req = request.Request(
            url=url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            },
            method="GET",
        )

        try:
            with self.urlopen_func(req, timeout=self.timeout_seconds, context=self._ssl_context()) as resp:
                raw = resp.read().decode("utf-8", errors="replace").strip()
                if not raw:
                    return {}
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                return {"data": parsed}
        except error.HTTPError as exc:
            if exc.code == 404:
                return {}
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Position re-sync failed with HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Position re-sync failed due to network error: {exc.reason}") from exc

    def _ssl_context(self) -> ssl.SSLContext | None:
        if self.base_url.lower().startswith("http://"):
            return None

        context = ssl.create_default_context()
        if not self.ssl_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context
