from __future__ import annotations

import logging
import threading
import traceback
import uuid
from typing import Any, Callable

from .queue import Job, JobQueue

log = logging.getLogger("jobqueue")
registry: dict[str, Callable[[Any], Any]] = {}


def task(name: str) -> Callable:
    """Register a function as the handler for a task name."""

    def decorate(fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
        registry[name] = fn
        return fn

    return decorate


class Worker:
    """Claims jobs, runs handlers, keeps the lease alive, reports the outcome."""

    def __init__(
        self,
        queue: JobQueue,
        *,
        queue_name: str = "default",
        lease_seconds: float = 30.0,
        poll_interval: float = 0.2,
        handlers: dict[str, Callable[[Any], Any]] | None = None,
    ) -> None:
        self.queue = queue
        self.queue_name = queue_name
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.handlers = handlers if handlers is not None else registry
        self.id = f"worker-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()

    def stop(self) -> None:
        """Graceful shutdown: finish the current job, then exit the loop."""
        self._stop.set()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(self.poll_interval)

    def run_once(self) -> bool:
        """Process at most one job. Returns False when the queue had nothing ready."""
        job = self.queue.claim(self.id, queue=self.queue_name, lease_seconds=self.lease_seconds)
        if job is None:
            return False
        self._execute(job)
        return True

    def _execute(self, job: Job) -> None:
        handler = self.handlers.get(job.task)
        if handler is None:
            self.queue.fail(job.id, self.id, f"no handler registered for task {job.task!r}")
            return

        done = threading.Event()

        def keep_alive() -> None:
            # Renew at a third of the lease so one missed beat is survivable.
            while not done.wait(self.lease_seconds / 3):
                if not self.queue.heartbeat(job.id, self.id, self.lease_seconds):
                    log.warning("lost lease on job %s", job.id)
                    return

        beat = threading.Thread(target=keep_alive, daemon=True)
        beat.start()
        try:
            result = handler(job.payload)
        except Exception as exc:  # noqa: BLE001 - any handler error is a job failure
            state = self.queue.fail(job.id, self.id, f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}")
            log.info("job %s failed (attempt %d) -> %s", job.id, job.attempts, state)
        else:
            if not self.queue.complete(job.id, self.id, result):
                log.warning("job %s finished after its lease was lost; result discarded", job.id)
        finally:
            done.set()
            beat.join(timeout=1)
