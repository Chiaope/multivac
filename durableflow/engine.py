"""DurableFlow engine: runs each job's steps in order, applying every credit at
most once through crashes, retries, restarts and racing workers.

Design in one paragraph. A tick is three short SQLite transactions with the
gateway call and the hooks in between, never inside one:

  1. claim    pick the earliest eligible job, bump its fencing token, take a
              lease (BEGIN IMMEDIATE makes this atomic across engines and
              processes);
  2. confirm  just before the effect, check the token is still ours and renew
              the lease; a worker whose claim was taken over stops here;
  3. finalize record the result only if the token is still ours.

Ownership is the token, not the worker name: several hosts in production ran
under the same worker ID. A step that was attempted before (a crash, a lost
acknowledgment, a takeover) asks the gateway `lookup(key)` before crediting
again, because the gateway forgets idempotency keys after a while and a late
retry would otherwise pay twice.
"""

import hashlib
import json
import sqlite3

from .model import (
    FAILED,
    FAILED_OUTCOME,
    IDLE,
    PENDING,
    PROGRESS,
    RETRY,
    RUNNING,
    SUCCEEDED,
    SUCCEEDED_OUTCOME,
    ConflictError,
    JobSnapshot,
    JobSpec,
    PermanentGatewayError,
    Step,
    ThrottledGatewayError,
    TickOutcome,
    TransientGatewayError,
    stable_key,
)

BUSY_TIMEOUT_S = 30
RETRY_CAP_FACTOR = 30  # longest wait is retry_base_seconds * 30 (60 s by default)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id      TEXT PRIMARY KEY,
    seq         INTEGER NOT NULL UNIQUE,     -- submission order
    steps       TEXT NOT NULL,               -- JSON [[account, amount], ...]
    step_count  INTEGER NOT NULL,
    status      TEXT NOT NULL,
    step_index  INTEGER NOT NULL DEFAULT 0,  -- next unfinished step
    failures    INTEGER NOT NULL DEFAULT 0,  -- transient errors on this step
    throttles   INTEGER NOT NULL DEFAULT 0,  -- rate-limit refusals on this step
    attempts    INTEGER NOT NULL DEFAULT 0,  -- claims of this step
    ready_at    REAL,                        -- set only while waiting to retry
    last_error  TEXT,
    token       INTEGER NOT NULL DEFAULT 0,  -- fencing token, bumped per claim
    owner       TEXT,
    lease_until REAL
);
CREATE INDEX IF NOT EXISTS jobs_open ON jobs (status, seq);
"""


class Engine:
    def __init__(
        self,
        state_db_path,
        gateway,
        clock,
        lease_seconds=10.0,
        max_failures=3,
        retry_base_seconds=2.0,
    ):
        self.path = state_db_path
        self.gateway = gateway
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.max_failures = max_failures
        self.retry_base_seconds = retry_base_seconds
        with self._db() as db:
            db.executescript(_SCHEMA)

    # ------------------------------------------------------------ public API
    def submit(self, spec: JobSpec) -> bool:
        steps = json.dumps([[s.account, s.amount] for s in spec.steps])
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT steps FROM jobs WHERE job_id = ?", (spec.job_id,)
            ).fetchone()
            if row is not None:
                db.execute("COMMIT")
                if row[0] != steps:
                    raise ConflictError(f"job {spec.job_id} exists with other steps")
                return False
            status = SUCCEEDED if not spec.steps else PENDING
            db.execute(
                "INSERT INTO jobs (job_id, seq, steps, step_count, status) "
                "SELECT ?, COALESCE(MAX(seq), 0) + 1, ?, ?, ? FROM jobs",
                (spec.job_id, steps, len(spec.steps), status),
            )
            db.execute("COMMIT")
        return True

    def tick(self, worker_id: str, hook=None) -> TickOutcome:
        hook = hook or (lambda stage, job_id, step_index: None)
        claim = self._claim(worker_id)
        if claim is None:
            return TickOutcome(IDLE)
        job_id, i, step, token, attempted_before = claim
        key = stable_key(job_id, i)

        hook("after_claim", job_id, i)
        hook("before_effect", job_id, i)
        if not self._confirm(job_id, token):
            return TickOutcome(IDLE)  # taken over while the hooks ran
        try:
            if not (attempted_before and self.gateway.lookup(key) is not None):
                self.gateway.credit(step.account, step.amount, key)
        except (TransientGatewayError, PermanentGatewayError) as e:
            hook("before_finalize", job_id, i)
            kind = self._record_error(job_id, i, token, e)
            hook("after_finalize", job_id, i)
            return TickOutcome(kind, job_id, i) if kind else TickOutcome(IDLE)

        hook("after_effect", job_id, i)
        hook("before_finalize", job_id, i)
        kind = self._record_done(job_id, i, token)
        hook("after_finalize", job_id, i)
        return TickOutcome(kind, job_id, i) if kind else TickOutcome(IDLE)

    def get(self, job_id: str) -> JobSnapshot | None:
        with self._db() as db:
            row = db.execute(
                "SELECT job_id, status, step_index, step_count, failures, ready_at, "
                "last_error FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return JobSnapshot(*row) if row else None

    # ------------------------------------------------------------ the three transactions
    def _claim(self, worker_id):
        """Claim the earliest eligible job: (job_id, i, step, token, attempted_before)."""
        now = self.clock.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT job_id, steps, step_index, token, attempts FROM jobs "
                "WHERE status IN (?, ?) "
                "AND (lease_until IS NULL OR lease_until <= ?) "
                "AND (ready_at IS NULL OR ready_at <= ?) "
                "ORDER BY seq LIMIT 1",
                (PENDING, RUNNING, now, now),
            ).fetchone()
            if row is None:
                db.execute("COMMIT")
                return None
            job_id, steps, i, token, attempts = row
            token += 1
            db.execute(
                "UPDATE jobs SET status = ?, token = ?, owner = ?, lease_until = ?, "
                "ready_at = NULL, attempts = attempts + 1 WHERE job_id = ?",
                (RUNNING, token, worker_id, now + self.lease_seconds, job_id),
            )
            db.execute("COMMIT")
        account, amount = json.loads(steps)[i]
        return job_id, i, Step(account, amount), token, attempts > 0

    def _confirm(self, job_id, token):
        """True if our claim still stands; renews the lease from now."""
        now = self.clock.now()
        with self._db() as db:
            cur = db.execute(
                "UPDATE jobs SET lease_until = ? WHERE job_id = ? AND token = ?",
                (now + self.lease_seconds, job_id, token),
            )
        return cur.rowcount == 1

    def _record_done(self, job_id, i, token):
        """Move past step i; the outcome kind, or None if our claim was lost."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT step_count FROM jobs WHERE job_id = ? AND token = ? "
                "AND step_index = ?",
                (job_id, token, i),
            ).fetchone()
            if row is None:
                db.execute("COMMIT")
                return None
            done = i + 1 == row[0]
            db.execute(
                "UPDATE jobs SET status = ?, step_index = ?, failures = 0, throttles = 0, "
                "attempts = 0, ready_at = NULL, last_error = NULL, owner = NULL, "
                "lease_until = NULL WHERE job_id = ?",
                (SUCCEEDED if done else PENDING, i + 1, job_id),
            )
            db.execute("COMMIT")
        return SUCCEEDED_OUTCOME if done else PROGRESS

    def _record_error(self, job_id, i, token, error):
        """Persist a gateway error on step i; the outcome kind, or None if our
        claim was lost (a stale worker records nothing)."""
        now = self.clock.now()
        key = stable_key(job_id, i)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT failures, throttles FROM jobs WHERE job_id = ? "
                "AND token = ? AND step_index = ?",
                (job_id, token, i),
            ).fetchone()
            if row is None:
                db.execute("COMMIT")
                return None
            failures, throttles = row
            if isinstance(error, PermanentGatewayError):
                status, ready_at = FAILED, None
            elif isinstance(error, ThrottledGatewayError):
                # A refusal, not a failure: nothing was done, so it does not
                # count toward max_failures; back off harder instead.
                throttles += 1
                status, ready_at = PENDING, now + self._delay(key, failures + throttles)
            else:
                failures += 1
                if failures >= self.max_failures:
                    status, ready_at = FAILED, None
                else:
                    status, ready_at = PENDING, now + self._delay(
                        key, failures + throttles
                    )
            db.execute(
                "UPDATE jobs SET status = ?, failures = ?, throttles = ?, ready_at = ?, "
                "last_error = ?, owner = NULL, lease_until = NULL WHERE job_id = ?",
                (status, failures, throttles, ready_at, str(error), job_id),
            )
            db.execute("COMMIT")
        return FAILED_OUTCOME if status == FAILED else RETRY

    # ------------------------------------------------------------ helpers
    def _delay(self, key, n):
        """Wait before retry n (1, 2, ...): exponential from retry_base_seconds,
        capped, with deterministic jitter in [50%, 100%] so that jobs failing
        together do not come back together. Always > 0."""
        ceiling = min(
            self.retry_base_seconds * 2 ** (n - 1),
            self.retry_base_seconds * RETRY_CAP_FACTOR,
        )
        digest = hashlib.sha256(f"{key}/{n}".encode()).digest()
        jitter = 0.5 + 0.5 * int.from_bytes(digest[:4], "big") / 2**32
        return max(ceiling * jitter, 1e-3)

    def _db(self):
        db = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_S, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        return _Closing(db)


class _Closing:
    """A connection closed when the block ends; an open transaction is rolled back."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, *exc):
        if self.db.in_transaction:
            self.db.execute("ROLLBACK")
        self.db.close()
