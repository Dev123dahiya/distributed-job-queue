import threading

import pytest

from jobqueue import JobQueue, JobState, Worker


class FakeClock:
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def queue(tmp_path, clock):
    return JobQueue(str(tmp_path / "jobs.db"), clock=clock, backoff_base=2.0, backoff_cap=60.0, jitter=lambda: 1.0)


def test_enqueue_claim_complete(queue):
    job_id = queue.enqueue("send_email", {"to": "a@example.com"})
    job = queue.claim("w1")
    assert job.id == job_id and job.payload == {"to": "a@example.com"} and job.attempts == 1
    assert queue.complete(job_id, "w1", {"sent": True})
    done = queue.get(job_id)
    assert done.state is JobState.SUCCEEDED and done.result == {"sent": True}
    assert queue.claim("w1") is None


def test_priority_then_fifo(queue, clock):
    low = queue.enqueue("t", "low")
    clock.now += 1
    high = queue.enqueue("t", "high", priority=10)
    clock.now += 1
    low2 = queue.enqueue("t", "low2")
    assert [queue.claim("w").id for _ in range(3)] == [high, low, low2]


def test_delayed_job_is_not_claimable_early(queue, clock):
    queue.enqueue("t", delay=30)
    assert queue.claim("w") is None
    clock.now += 30
    assert queue.claim("w") is not None


def test_idempotency_key_dedupes(queue):
    first = queue.enqueue("charge", {"order": 7}, idempotency_key="order-7")
    second = queue.enqueue("charge", {"order": 7}, idempotency_key="order-7")
    assert first == second and queue.stats()["queued"] == 1


def test_failure_retries_with_exponential_backoff(queue, clock):
    job_id = queue.enqueue("t", max_attempts=4)
    delays = []
    for _ in range(3):
        queue.claim("w")
        assert queue.fail(job_id, "w", "boom") is JobState.QUEUED
        delays.append(queue.get(job_id).run_at - clock.now)
        assert queue.claim("w") is None  # still backing off
        clock.now += delays[-1]
    assert delays == [2.0, 4.0, 8.0]


def test_backoff_is_capped(queue):
    assert queue.backoff(20) == 60.0


def test_dead_letter_after_max_attempts_and_requeue(queue, clock):
    job_id = queue.enqueue("t", max_attempts=2)
    queue.claim("w")
    queue.fail(job_id, "w", "first")
    clock.now += 10
    queue.claim("w")
    assert queue.fail(job_id, "w", "second") is JobState.DEAD
    dead = queue.dead_letters()
    assert [j.id for j in dead] == [job_id] and dead[0].last_error == "second"
    assert queue.requeue_dead(job_id)
    assert queue.claim("w").attempts == 1


def test_expired_lease_is_reclaimed_and_stale_worker_is_fenced(queue, clock):
    job_id = queue.enqueue("t")
    queue.claim("crashed", lease_seconds=30)
    assert queue.claim("other") is None  # lease still valid
    clock.now += 31
    again = queue.claim("other")
    assert again.id == job_id and again.attempts == 2 and again.lease_owner == "other"
    # The first worker wakes up late: its writes must be rejected.
    assert not queue.complete(job_id, "crashed", "stale")
    assert queue.fail(job_id, "crashed", "stale") is None
    assert not queue.heartbeat(job_id, "crashed")
    assert queue.complete(job_id, "other", "fresh")
    assert queue.get(job_id).result == "fresh"


def test_heartbeat_extends_lease(queue, clock):
    job_id = queue.enqueue("t")
    queue.claim("w", lease_seconds=30)
    clock.now += 20
    assert queue.heartbeat(job_id, "w", lease_seconds=30)
    clock.now += 20  # 40 s after claim, but the lease was renewed at 20 s
    assert queue.claim("thief") is None


def test_queues_are_isolated(queue):
    queue.enqueue("t", queue="emails")
    assert queue.claim("w", queue="reports") is None
    assert queue.claim("w", queue="emails") is not None


def test_worker_runs_handler_and_records_failures(queue, clock):
    calls = []

    def handler(payload):
        calls.append(payload)
        if payload == "bad":
            raise ValueError("nope")
        return payload.upper()

    ok = queue.enqueue("t", "good")
    bad = queue.enqueue("t", "bad", max_attempts=1)
    missing = queue.enqueue("unknown_task", max_attempts=1)
    worker = Worker(queue, handlers={"t": handler})
    while worker.run_once():
        pass
    assert queue.get(ok).result == "GOOD"
    assert queue.get(bad).state is JobState.DEAD and "ValueError: nope" in queue.get(bad).last_error
    assert "no handler registered" in queue.get(missing).last_error


def test_no_job_is_claimed_twice_under_contention(tmp_path):
    """8 threads race to claim 300 jobs from one database file."""
    queue = JobQueue(str(tmp_path / "race.db"))
    ids = {queue.enqueue("t", n) for n in range(300)}
    claimed: list[str] = []
    lock = threading.Lock()
    start = threading.Barrier(8)

    def worker(name):
        start.wait()
        while (job := queue.claim(name, lease_seconds=600)) is not None:
            with lock:
                claimed.append(job.id)
            queue.complete(job.id, name)

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 300 and set(claimed) == ids
    assert queue.stats()["succeeded"] == 300
