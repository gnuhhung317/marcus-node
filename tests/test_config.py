from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from local_executor.config import ExecutorConfig


class ExecutorConfigTest(unittest.TestCase):
    def test_from_env_loads_optional_telegram_settings(self) -> None:
        env = {
            "SYSTEM_WS_URL": "ws://localhost/ws",
            "SYSTEM_WS_TOKEN": "ws-token",
            "BOT_ID": "bot-01",
            "DEFAULT_ORDER_AMOUNT": "0.01",
            "EXECUTION_MODE": "dry-run",
            "TELEGRAM_BOT_TOKEN": "telegram-token",
            "TELEGRAM_CHAT_ID": "123456",
        }

        with patch.dict(os.environ, env, clear=True):
            config = ExecutorConfig.from_env()

        self.assertEqual(config.telegram_bot_token, "telegram-token")
        self.assertEqual(config.telegram_chat_id, "123456")


if __name__ == "__main__":
    unittest.main()
