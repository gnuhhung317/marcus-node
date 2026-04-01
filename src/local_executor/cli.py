from __future__ import annotations

import asyncio
import logging
import signal

from .config import ExecutorConfig
from .engine import LocalExecutorEngine
from .env_loader import load_env_file


def main() -> None:
    asyncio.run(_run())


async def _run() -> None:
    load_env_file()
    config = ExecutorConfig.from_env()
    _configure_logging(config.log_level)

    logger = logging.getLogger(__name__)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows event loop may not support signal handlers.
            pass

    engine = LocalExecutorEngine(config=config)
    logger.info("Local Executor started for bot_id=%s", config.bot_id)

    try:
        await engine.run(stop_event=stop_event)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
