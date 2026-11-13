from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    queue           TEXT NOT NULL,
    task            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    state           TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL,
    run_at          REAL NOT NULL,
    lease_until     REAL,
    lease_owner     TEXT,
    idempotency_key TEXT,
    last_error      TEXT,
    result          TEXT,
    created_at      REAL NOT NULL,
    finished_at     REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idem ON jobs(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue, state, run_at, priority);
"""


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"


@dataclass(frozen=True)
class Job:
    id: str
    queue: str
    task: str
    payload: Any
    state: JobState
    priority: int
    attempts: int
    max_attempts: int
    run_at: float
    lease_until: float | None
    lease_owner: str | None
    last_error: str | None
    result: Any


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"], queue=row["queue"], task=row["task"], payload=json.loads(row["payload"]),
        state=JobState(row["state"]), priority=row["priority"], attempts=row["attempts"],
        max_attempts=row["max_attempts"], run_at=row["run_at"], lease_until=row["lease_until"],
        lease_owner=row["lease_owner"], last_error=row["last_error"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
    )


class JobQueue:
    """Queue operations. Each one is a single SQLite transaction.

    Delivery is at-least-once: a worker that dies mid-job loses its lease and the
    job is handed to someone else. Handlers therefore need to be idempotent.
    """

    def __init__(
        self,
        path: str,
        clock: Callable[[], float] = time.time,
        backoff_base: float = 2.0,
        backoff_cap: float = 300.0,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self.path = path
        self._clock = clock
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._jitter = jitter
        self._local = threading.local()
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    # ---- producing -------------------------------------------------------
    def enqueue(
        self,
        task: str,
        payload: Any = None,
        *,
        queue: str = "default",
        priority: int = 0,
        delay: float = 0.0,
        max_attempts: int = 5,
        idempotency_key: str | None = None,
    ) -> str:
        """Add a job. With an idempotency key, a repeat call returns the first job's id."""
        now = self._clock()
        job_id = uuid.uuid4().hex
        conn = self._conn()
        try:
            conn.execute(
                "INSERT INTO jobs (id, queue, task, payload, state, priority, max_attempts, run_at, idempotency_key, created_at)"
                " VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)",
                (job_id, queue, task, json.dumps(payload), priority, max_attempts, now + delay, idempotency_key, now),
            )
            return job_id
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT id FROM jobs WHERE queue = ? AND idempotency_key = ?", (queue, idempotency_key)
            ).fetchone()
            return row["id"]

    # ---- consuming -------------------------------------------------------
    def claim(self, worker_id: str, *, queue: str = "default", lease_seconds: float = 30.0) -> Job | None:
        """Atomically take the best ready job: highest priority, then oldest.

        A job is ready if it is queued and due, or running with an expired lease
        (its previous worker is presumed dead).
        """
        now = self._clock()
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")  # take the write lock before reading
        try:
            row = conn.execute(
                "SELECT id FROM jobs WHERE queue = ? AND ("
                " (state = 'queued' AND run_at <= ?) OR (state = 'running' AND lease_until <= ?))"
                " ORDER BY priority DESC, run_at ASC, created_at ASC LIMIT 1",
                (queue, now, now),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute(
                "UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_until = ?, lease_owner = ? WHERE id = ?",
                (now + lease_seconds, worker_id, row["id"]),
            )
            job = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            conn.execute("COMMIT")
            return _row_to_job(job)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def heartbeat(self, job_id: str, worker_id: str, lease_seconds: float = 30.0) -> bool:
        """Extend a lease. False means the lease was lost and the worker must stop."""
        cur = self._conn().execute(
            "UPDATE jobs SET lease_until = ? WHERE id = ? AND lease_owner = ? AND state = 'running' AND lease_until > ?",
            (self._clock() + lease_seconds, job_id, worker_id, self._clock()),
        )
        return cur.rowcount == 1

    def complete(self, job_id: str, worker_id: str, result: Any = None) -> bool:
        """Mark success. Ignored (False) if this worker no longer holds the lease."""
        cur = self._conn().execute(
            "UPDATE jobs SET state = 'succeeded', result = ?, finished_at = ?, lease_until = NULL"
            " WHERE id = ? AND lease_owner = ? AND state = 'running'",
            (json.dumps(result), self._clock(), job_id, worker_id),
        )
        return cur.rowcount == 1

    def fail(self, job_id: str, worker_id: str, error: str) -> JobState | None:
        """Record a failure: retry with exponential backoff, or dead-letter when out of attempts."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE id = ? AND lease_owner = ? AND state = 'running'",
                (job_id, worker_id),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            now = self._clock()
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    "UPDATE jobs SET state = 'dead', last_error = ?, finished_at = ?, lease_until = NULL WHERE id = ?",
                    (error, now, job_id),
                )
                state = JobState.DEAD
            else:
                conn.execute(
                    "UPDATE jobs SET state = 'queued', last_error = ?, run_at = ?, lease_until = NULL, lease_owner = NULL WHERE id = ?",
                    (error, now + self.backoff(row["attempts"]), job_id),
                )
                state = JobState.QUEUED
            conn.execute("COMMIT")
            return state
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def backoff(self, attempts: int) -> float:
        """Exponential backoff with full jitter: uniform in [0, min(cap, base * 2^(n-1))]."""
        ceiling = min(self._backoff_cap, self._backoff_base * (2 ** (attempts - 1)))
        return ceiling * self._jitter()

    # ---- operating -------------------------------------------------------
    def get(self, job_id: str) -> Job | None:
        row = self._conn().execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row else None

    def stats(self, queue: str = "default") -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT state, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY state", (queue,)
        ).fetchall()
        counts = {s.value: 0 for s in JobState}
        counts.update({r["state"]: r["n"] for r in rows})
        return counts

    def dead_letters(self, queue: str = "default") -> list[Job]:
        rows = self._conn().execute(
            "SELECT * FROM jobs WHERE queue = ? AND state = 'dead' ORDER BY finished_at", (queue,)
        ).fetchall()
        return [_row_to_job(r) for r in rows]

    def requeue_dead(self, job_id: str) -> bool:
        """Give a dead job a fresh set of attempts."""
        cur = self._conn().execute(
            "UPDATE jobs SET state = 'queued', attempts = 0, run_at = ?, last_error = NULL, finished_at = NULL,"
            " lease_owner = NULL WHERE id = ? AND state = 'dead'",
            (self._clock(), job_id),
        )
        return cur.rowcount == 1
