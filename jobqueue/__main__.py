"""CLI: python -m jobqueue {stats,dead,requeue,demo} --db jobs.db"""
from __future__ import annotations

import argparse
import json
import threading
import time

from .queue import JobQueue
from .worker import Worker


def demo(db: str, jobs: int, workers: int) -> None:
    """Enqueue jobs where 1 in 5 fails twice, run a worker pool, print what happened."""
    queue = JobQueue(db, backoff_base=0.05, backoff_cap=0.2)
    seen: dict[int, int] = {}
    lock = threading.Lock()

    def flaky(payload: dict) -> int:
        n = payload["n"]
        with lock:
            seen[n] = seen.get(n, 0) + 1
            attempt = seen[n]
        if n % 5 == 0 and attempt <= 2:
            raise RuntimeError("simulated transient failure")
        if n % 50 == 0:
            raise ValueError("simulated permanent failure")
        return n * n

    for n in range(1, jobs + 1):
        queue.enqueue("flaky", {"n": n}, idempotency_key=f"demo-{n}", max_attempts=4)
    pool = [Worker(queue, handlers={"flaky": flaky}, poll_interval=0.02) for _ in range(workers)]
    threads = [threading.Thread(target=w.run_forever) for w in pool]
    start = time.perf_counter()
    for t in threads:
        t.start()
    while True:
        stats = queue.stats()
        if stats["queued"] == 0 and stats["running"] == 0:
            break
        time.sleep(0.05)
    for w in pool:
        w.stop()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    print(json.dumps({"jobs": jobs, "workers": workers, "seconds": round(elapsed, 2),
                      "handler_runs": sum(seen.values()), **queue.stats()}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="jobqueue")
    parser.add_argument("command", choices=["stats", "dead", "requeue", "demo"])
    parser.add_argument("job_id", nargs="?")
    parser.add_argument("--db", default="jobs.db")
    parser.add_argument("--queue", default="default")
    parser.add_argument("--jobs", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    if args.command == "demo":
        demo(args.db, args.jobs, args.workers)
        return
    queue = JobQueue(args.db)
    if args.command == "stats":
        print(json.dumps(queue.stats(args.queue), indent=2))
    elif args.command == "dead":
        for job in queue.dead_letters(args.queue):
            print(job.id, job.task, json.dumps(job.payload), "|", (job.last_error or "").splitlines()[0])
    elif args.command == "requeue":
        print("requeued" if queue.requeue_dead(args.job_id) else "not found or not dead")


if __name__ == "__main__":
    main()
