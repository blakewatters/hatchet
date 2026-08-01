from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import cast

from hatchet_sdk.clients.events import EventClient
from hatchet_sdk.runnables.contextvars import ctx_step_run_id, ctx_task_retry_count
from hatchet_sdk.utils.typing import LogLevel
from hatchet_sdk.worker.runner.utils.capture_logs import (
    AsyncLogSender,
    LogForwardingHandler,
    LogRecord,
    capture_logs,
)


class FakeEventClient:
    def __init__(self) -> None:
        self.client_config = SimpleNamespace(log_queue_size=10)


async def test_log_forwarding_handler_enqueues_correct_record() -> None:
    event_client = FakeEventClient()
    log_sender = AsyncLogSender(cast(EventClient, event_client))

    target_logger = logging.getLogger("capture-log-test")
    previous_level = target_logger.level
    target_logger.setLevel(logging.INFO)

    handler = LogForwardingHandler(log_sender)
    target_logger.addHandler(handler)

    step_token = ctx_step_run_id.set("step-run-id")
    retry_token = ctx_task_retry_count.set(2)

    try:

        def log_from_worker_thread() -> None:
            logging.getLogger("capture-log-test").info("hello from worker thread")

        await asyncio.to_thread(log_from_worker_thread)

        record = log_sender.q.get()
        assert isinstance(record, LogRecord)
        assert record.message == "hello from worker thread"
        assert record.step_run_id == "step-run-id"
        assert record.level == LogLevel.INFO
        assert record.task_retry_count == 2
    finally:
        ctx_step_run_id.reset(step_token)
        ctx_task_retry_count.reset(retry_token)
        target_logger.removeHandler(handler)
        target_logger.setLevel(previous_level)


async def test_capture_logs_does_not_retain_records() -> None:
    """The handler must not hold on to records.

    capture_logs wraps the worker's whole action run loop, so anything the
    handler retains is retained for the lifetime of the worker process. It
    previously subclassed logging.StreamHandler over a StringIO that nothing
    ever read, so a worker's memory grew with everything it logged.
    """
    event_client = FakeEventClient()
    log_sender = AsyncLogSender(cast(EventClient, event_client))

    target_logger = logging.getLogger("capture-log-retention-test")
    target_logger.setLevel(logging.INFO)
    target_logger.propagate = False

    handlers: list[logging.Handler] = []

    async def run_loop() -> None:
        handlers.extend(target_logger.handlers)
        # No step run is set, so every one of these is a record the handler
        # drops rather than forwards -- it must not retain them either.
        for _ in range(10_000):
            target_logger.info("x" * 1_024)

    await capture_logs(target_logger, log_sender, run_loop)()

    (handler,) = handlers
    assert isinstance(handler, LogForwardingHandler)
    # The handler holds no stream to accumulate into.
    assert not hasattr(handler, "stream")
    assert log_sender.q.empty()
    # And capture_logs cleans up after itself.
    assert target_logger.handlers == []
