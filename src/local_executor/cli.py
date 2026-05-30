from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from .config import ExecutorConfig
from .engine import LocalExecutorEngine
from .env_loader import load_env_file
from .local_store import LocalExecutionStore


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

    db_path = Path(os.getenv("EXECUTOR_DB_PATH", "executor_state.db"))
    local_store = LocalExecutionStore(db_path=db_path, logger=logging.getLogger("local_store"))

    engine = LocalExecutorEngine(config=config, local_store=local_store)
    logger.info("Local Executor started bot_id=%s db=%s", config.bot_id, db_path)

    try:
        await engine.run(stop_event=stop_event)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")




def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
