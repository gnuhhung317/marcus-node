from __future__ import annotations

import unittest

from local_executor.notifications import TelegramNotifier, build_executor_alert


class TelegramNotifierTest(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_notifier_is_noop(self) -> None:
        notifier = TelegramNotifier(bot_token=None, chat_id=None)

        self.assertFalse(notifier.enabled)
        self.assertFalse(await notifier.send("test"))
        self.assertIsNone(notifier.notify("test"))

    def test_build_executor_alert_formats_fields(self) -> None:
        message = build_executor_alert(
            "Execution error",
            bot_id="bot-01",
            signal_id="sig-1",
            errors="order rejected",
            empty="",
        )

        self.assertIn("[Marcus Local Executor] Execution error", message)
        self.assertIn("bot id: bot-01", message)
        self.assertIn("signal id: sig-1", message)
        self.assertIn("errors: order rejected", message)
        self.assertNotIn("empty:", message)


if __name__ == "__main__":
    unittest.main()
