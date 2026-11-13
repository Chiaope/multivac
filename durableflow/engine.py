"""The supplied engine: a first version that runs jobs whose steps succeed.

It does not meet the rules of the brief's section 3, and it holds a database
transaction while the gateway and the hooks run, which section 4 forbids.
You may replace this file. Keep the public signatures of Engine.
"""
import json
import sqlite3

from .model import (FAILED, FAILED_OUTCOME, IDLE, PENDING, PROGRESS, RETRY, RUNNING,
                    SUCCEEDED, SUCCEEDED_OUTCOME, JobSnapshot, JobSpec,
                    PermanentGatewayError, Step, TickOutcome, TransientGatewayError,
                    stable_key)


class Engine:
    def __init__(self, state_db_path, gateway, clock, lease_seconds=10.0,
                 max_failures=3, retry_base_seconds=2.0):
        self.gateway = gateway
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.max_failures = max_failures
        self.retry_base_seconds = retry_base_seconds
        self.db = sqlite3.connect(state_db_path, timeout=5)
        with self.db:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    steps TEXT NOT NULL,
                    status TEXT NOT NULL,
                    step_index INTEGER NOT NULL DEFAULT 0,
                    failures INTEGER NOT NULL DEFAULT 0,
                    ready_at REAL,
                    last_error TEXT)""")

    def submit(self, spec: JobSpec) -> bool:
        steps = json.dumps([[s.account, s.amount] for s in spec.steps])
        with self.db:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO jobs (job_id, seq, steps, status) "
                "SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ? FROM jobs",
                (spec.job_id, steps, PENDING))
        return cur.rowcount == 1

    def tick(self, worker_id: str, hook=None) -> TickOutcome:
        hook = hook or (lambda stage, job_id, step_index: None)
        with self.db:
            row = self.db.execute(
                "SELECT job_id, steps, step_index, failures FROM jobs "
                "WHERE status = ? ORDER BY seq LIMIT 1", (PENDING,)).fetchone()
            if row is None:
                return TickOutcome(IDLE)
            job_id, steps, i, failures = row
            steps = [Step(*s) for s in json.loads(steps)]
            self.db.execute("UPDATE jobs SET status = ? WHERE job_id = ?", (RUNNING, job_id))
            hook("after_claim", job_id, i)
            step = steps[i]
            hook("before_effect", job_id, i)
            try:
                self.gateway.credit(step.account, step.amount, stable_key(job_id, i))
            except TransientGatewayError as e:
                failures += 1
                status = FAILED if failures >= self.max_failures else PENDING
                self.db.execute("UPDATE jobs SET status = ?, failures = ?, last_error = ? "
                                "WHERE job_id = ?", (status, failures, str(e), job_id))
                return TickOutcome(FAILED_OUTCOME if status == FAILED else RETRY, job_id, i)
            except PermanentGatewayError as e:
                self.db.execute("UPDATE jobs SET status = ?, last_error = ? WHERE job_id = ?",
                                (FAILED, str(e), job_id))
                return TickOutcome(FAILED_OUTCOME, job_id, i)
            hook("after_effect", job_id, i)
            hook("before_finalize", job_id, i)
            done = i + 1 == len(steps)
            self.db.execute("UPDATE jobs SET status = ?, step_index = ? WHERE job_id = ?",
                            (SUCCEEDED if done else PENDING, i + 1, job_id))
            hook("after_finalize", job_id, i)
            return TickOutcome(SUCCEEDED_OUTCOME if done else PROGRESS, job_id, i)

    def get(self, job_id: str) -> JobSnapshot | None:
        row = self.db.execute(
            "SELECT job_id, status, step_index, steps, failures, ready_at, last_error "
            "FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return JobSnapshot(row[0], row[1], row[2], len(json.loads(row[3])), row[4], row[5],
                           row[6])
