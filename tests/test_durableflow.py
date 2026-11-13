"""Additional tests for DurableFlow covering crashes, fencing, key TTL expiry,
outages, hook concurrency, and multi-process workers.

Uses only the public API and drives retries from `get(...).ready_at`.
"""
import gc
import multiprocessing
import os
import tempfile
import unittest

from durableflow import (
    FAILED,
    PENDING,
    RUNNING,
    SUCCEEDED,
    Clock,
    ConflictError,
    Engine,
    FakeClock,
    Gateway,
    JobSpec,
    Step,
    stable_key,
)


class SharedFileClock(Clock):
    """A Clock backed by a file so multiple processes see the same time."""

    def __init__(self, path: str):
        self.path = path

    def now(self) -> float:
        with open(self.path, "r", encoding="utf-8") as f:
            return float(f.read().strip())

    def set(self, t: float) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(repr(float(t)))


def _mp_worker(state_path, gateway_path, clock_path, worker_id, job_count, q):
    """Top-level worker function for multiprocessing (picklable on Windows)."""
    clock = SharedFileClock(clock_path)
    gateway = Gateway(gateway_path, clock)
    engine = Engine(state_path, gateway, clock)
    outcomes = []
    # Run until all jobs reach a terminal state or we hit a generous cap.
    for _ in range(job_count * 10):
        out = engine.tick(worker_id)
        if out.kind != "idle":
            outcomes.append(out)
        done = sum(
            1
            for j in range(job_count)
            if (s := engine.get(f"mp-{j}")) is not None
            and s.status in (SUCCEEDED, FAILED)
        )
        if done == job_count:
            break
    q.put(outcomes)


def _crash_at(stage, job_id=None, step_index=None):
    def hook(s, j, i):
        if s == stage and job_id in (None, j) and step_index in (None, i):
            raise SystemExit(f"crash at {stage}")
    return hook


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.state = os.path.join(self.dir.name, "state.db")
        self.gw_path = os.path.join(self.dir.name, "gateway.db")
        self.clock = FakeClock(1000.0)
        self.gateway = Gateway(self.gw_path, self.clock)

    def tearDown(self):
        gc.collect()
        self.dir.cleanup()

    def make_engine(self, **kw):
        return Engine(self.state, self.gateway, self.clock, **kw)

    def crash(self, engine, stage, worker="w1", job_id=None, step_index=None):
        with self.assertRaises(SystemExit):
            engine.tick(worker, hook=_crash_at(stage, job_id, step_index))
        del engine
        gc.collect()


class CrashAndKeyTTLTests(BaseCase):
    def test_crash_at_every_stage_credits_once(self):
        for stage in ("after_claim", "before_effect", "after_effect",
                      "before_finalize", "after_finalize"):
            with self.subTest(stage=stage):
                sub_dir = tempfile.TemporaryDirectory()
                try:
                    clock = FakeClock(1000.0)
                    gw = Gateway(os.path.join(sub_dir.name, "gw.db"), clock)
                    st = os.path.join(sub_dir.name, "st.db")
                    e = Engine(st, gw, clock)
                    e.submit(JobSpec("j1", (Step("alice", 10), Step("bob", 20))))

                    with self.assertRaises(SystemExit):
                        e.tick("w1", hook=_crash_at(stage, "j1", 0))
                    del e
                    gc.collect()

                    clock.advance(10.0)  # expire any lease
                    e2 = Engine(st, gw, clock)
                    for _ in range(10):
                        snap = e2.get("j1")
                        if snap.status == SUCCEEDED:
                            break
                        if snap.ready_at is not None:
                            clock.set(snap.ready_at)
                        e2.tick("w2")

                    self.assertEqual(e2.get("j1").status, SUCCEEDED)
                    self.assertEqual(gw.balance("alice"), 10)
                    self.assertEqual(gw.balance("bob"), 20)
                    self.assertEqual(gw.committed_keys(), ["j1#0", "j1#1"])
                finally:
                    del e2
                    gc.collect()
                    sub_dir.cleanup()

    def test_lost_ack_after_key_ttl_does_not_double_credit(self):
        gw = Gateway(self.gw_path, self.clock, key_ttl=50.0)
        e = Engine(self.state, gw, self.clock)
        e.submit(JobSpec("j-ttl", (Step("alice", 25),)))
        gw.set_plan(stable_key("j-ttl", 0), ["after", "ok"])

        self.assertEqual(e.tick("w1").kind, "retry")
        snap = e.get("j-ttl")
        self.assertEqual(snap.status, PENDING)
        self.assertIsNotNone(snap.ready_at)

        # Advance well past key_ttl=50s so gateway forgets the idempotency key.
        self.clock.set(max(snap.ready_at, 1000.0 + 100.0))
        self.assertEqual(e.tick("w1").kind, "succeeded")
        self.assertEqual(gw.balance("alice"), 25)
        self.assertEqual(gw.committed_keys(), ["j-ttl#0"])

    def test_crash_after_effect_and_late_restart_past_key_ttl(self):
        gw = Gateway(self.gw_path, self.clock, key_ttl=50.0)
        e = Engine(self.state, gw, self.clock)
        e.submit(JobSpec("j-late", (Step("bob", 30), Step("carol", 15))))

        self.crash(e, "after_effect", job_id="j-late", step_index=0)
        self.clock.advance(200.0)  # way past key_ttl and lease_seconds

        e2 = Engine(self.state, gw, self.clock)
        self.assertEqual(e2.tick("w2").kind, "progress")
        self.assertEqual(e2.tick("w2").kind, "succeeded")
        self.assertEqual(gw.balance("bob"), 30)
        self.assertEqual(gw.balance("carol"), 15)
        self.assertEqual(gw.committed_keys(), ["j-late#0", "j-late#1"])

    def test_lookup_transient_failure_recovers_without_double_credit(self):
        gw = Gateway(self.gw_path, self.clock, key_ttl=50.0)
        e = Engine(self.state, gw, self.clock)
        e.submit(JobSpec("j-lk", (Step("alice", 40),)))
        gw.set_plan(stable_key("j-lk", 0), ["after", "ok"])
        gw.set_lookup_plan(stable_key("j-lk", 0), ["fail", "ok"])

        self.assertEqual(e.tick("w1").kind, "retry")
        self.clock.set(max(e.get("j-lk").ready_at, 1100.0))
        # Lookup fails transiently on this attempt; must retry, not credit again.
        self.assertEqual(e.tick("w1").kind, "retry")
        self.clock.set(e.get("j-lk").ready_at)
        self.assertEqual(e.tick("w1").kind, "succeeded")
        self.assertEqual(gw.balance("alice"), 40)
        self.assertEqual(gw.committed_keys(), ["j-lk#0"])


class LeaseAndFencingTests(BaseCase):
    def test_lease_boundary_exact_deadline(self):
        e1 = self.make_engine(lease_seconds=10.0)
        e2 = self.make_engine(lease_seconds=10.0)
        e1.submit(JobSpec("j-lease", (Step("alice", 10),)))

        seen = []

        def check_at_boundary(stage, job_id, step_index):
            if stage == "after_claim":
                self.clock.advance(9.99)
                seen.append(("before", e2.tick("w2").kind))
                self.clock.set(1010.0)  # exact deadline: 1000.0 + 10.0
                seen.append(("at", e2.tick("w2").kind))

        out = e1.tick("w1", hook=check_at_boundary)
        self.assertEqual(seen, [("before", "idle"), ("at", "succeeded")])
        self.assertEqual(out.kind, "idle")
        self.assertEqual(self.gateway.balance("alice"), 10)

    def test_stale_worker_after_effect_does_not_overwrite(self):
        e1 = self.make_engine(lease_seconds=10.0)
        e2 = self.make_engine(lease_seconds=10.0)
        e1.submit(JobSpec("j-ae", (Step("alice", 10), Step("bob", 20))))

        seen = []

        def overtake(stage, job_id, step_index):
            if stage == "after_effect" and step_index == 0:
                self.clock.advance(11.0)
                seen.append(e2.tick("w2"))  # finishes step 0
                seen.append(e2.tick("w2"))  # finishes step 1

        out = e1.tick("w1", hook=overtake)
        self.assertEqual(out.kind, "idle")
        self.assertEqual([o.kind for o in seen], ["progress", "succeeded"])
        snap = e1.get("j-ae")
        self.assertEqual((snap.status, snap.step_index), (SUCCEEDED, 2))
        self.assertEqual(self.gateway.balance("alice"), 10)
        self.assertEqual(self.gateway.balance("bob"), 20)

    def test_stale_worker_with_same_worker_id_is_fenced(self):
        e1 = self.make_engine(lease_seconds=10.0)
        e2 = self.make_engine(lease_seconds=10.0)
        e1.submit(JobSpec("j-same-id", (Step("alice", 10), Step("bob", 20))))

        seen = []

        def overtake(stage, job_id, step_index):
            if stage == "before_effect":
                self.clock.advance(10.0)
                seen.append(e2.tick("worker-1"))

        out = e1.tick("worker-1", hook=overtake)
        self.assertEqual(out.kind, "idle")
        self.assertEqual(seen[0].kind, "progress")
        self.assertEqual(self.gateway.balance("alice"), 10)

    def test_stale_worker_error_does_not_increment_failures(self):
        e1 = self.make_engine(lease_seconds=10.0, max_failures=2)
        e2 = self.make_engine(lease_seconds=10.0, max_failures=2)
        e1.submit(JobSpec("j-err", (Step("alice", 10),)))
        self.gateway.set_plan(stable_key("j-err", 0), ["ok", "before"])

        # Let w1 get overtaken right after_claim; w2 succeeds step 0.
        # Then w1 would fail if it were not fenced, or even if it hits a gateway error.
        seen = []

        def overtake(stage, job_id, step_index):
            if stage == "before_effect":
                self.clock.advance(10.0)
                seen.append(e2.tick("w2"))  # consumes "ok", succeeds

        out = e1.tick("w1", hook=overtake)
        self.assertEqual(out.kind, "idle")
        self.assertEqual(seen[0].kind, "succeeded")
        snap = e1.get("j-err")
        self.assertEqual((snap.status, snap.failures), (SUCCEEDED, 0))


class SchedulingAndOutageTests(BaseCase):
    def test_fifo_order_and_skip_waiting_job(self):
        e = self.make_engine()
        e.submit(JobSpec("j-1", (Step("alice", 10),)))
        e.submit(JobSpec("j-2", (Step("bob", 20),)))
        self.gateway.set_plan(stable_key("j-1", 0), ["before"])

        self.assertEqual(e.tick("w1").job_id, "j-1")  # j-1 fails and waits
        self.assertEqual(e.get("j-1").status, PENDING)

        # j-1 is waiting for ready_at, so j-2 is eligible and runs next.
        out2 = e.tick("w1")
        self.assertEqual((out2.kind, out2.job_id), ("succeeded", "j-2"))

        ready_at = e.get("j-1").ready_at
        self.clock.set(ready_at - 0.001)
        self.assertEqual(e.tick("w1").kind, "idle")
        self.clock.set(ready_at)
        out1 = e.tick("w1")
        self.assertEqual((out1.kind, out1.job_id), ("succeeded", "j-1"))

    def test_ten_minute_outage_rides_out_with_max_failures_30(self):
        e = self.make_engine(max_failures=30, retry_base_seconds=2.0)
        e.submit(JobSpec("j-outage", (Step("alice", 50), Step("bob", 25))))
        self.gateway.set_outage(1000.0, 1600.0)

        for _ in range(100):
            snap = e.get("j-outage")
            if snap.status in (SUCCEEDED, FAILED):
                break
            if snap.ready_at is not None:
                self.clock.set(snap.ready_at)
            e.tick("w1")

        snap = e.get("j-outage")
        self.assertEqual(snap.status, SUCCEEDED)
        self.assertEqual(self.gateway.balance("alice"), 50)
        self.assertEqual(self.gateway.balance("bob"), 25)

    def test_hook_can_access_engine_without_db_lock(self):
        e1 = self.make_engine()
        e2 = self.make_engine()
        e1.submit(JobSpec("j-hook", (Step("alice", 10),)))

        snapshots = []

        def inspect_in_hook(stage, job_id, step_index):
            snapshots.append((stage, e2.get(job_id).status))
            if stage == "after_claim":
                e2.submit(JobSpec("j-extra", (Step("bob", 5),)))

        out = e1.tick("w1", hook=inspect_in_hook)
        self.assertEqual(out.kind, "succeeded")
        self.assertEqual(snapshots[0], ("after_claim", RUNNING))
        self.assertIsNotNone(e1.get("j-extra"))

    def test_resubmit_after_terminal_returns_false_or_conflict(self):
        e = self.make_engine()
        spec = JobSpec("j-done", (Step("alice", 10),))
        self.assertTrue(e.submit(spec))
        self.assertEqual(e.tick("w1").kind, "succeeded")
        self.assertFalse(e.submit(spec))
        with self.assertRaises(ConflictError):
            e.submit(JobSpec("j-done", (Step("alice", 99),)))


class MultiProcessTests(unittest.TestCase):
    def test_two_processes_racing_credit_each_step_once(self):
        with tempfile.TemporaryDirectory() as d:
            state = os.path.join(d, "state.db")
            gw_path = os.path.join(d, "gateway.db")
            clock_path = os.path.join(d, "clock.txt")

            clock = SharedFileClock(clock_path)
            clock.set(1000.0)
            gw = Gateway(gw_path, clock)
            e = Engine(state, gw, clock)

            job_count = 6
            for j in range(job_count):
                e.submit(JobSpec(f"mp-{j}", (Step("alice", 10), Step("bob", 5))))
            del e
            gc.collect()

            ctx = multiprocessing.get_context("spawn")
            q = ctx.Queue()
            p1 = ctx.Process(
                target=_mp_worker,
                args=(state, gw_path, clock_path, "proc-1", job_count, q),
            )
            p2 = ctx.Process(
                target=_mp_worker,
                args=(state, gw_path, clock_path, "proc-2", job_count, q),
            )
            p1.start()
            p2.start()
            p1.join(timeout=30)
            p2.join(timeout=30)
            self.assertEqual(p1.exitcode, 0)
            self.assertEqual(p2.exitcode, 0)

            all_outcomes = q.get(timeout=5) + q.get(timeout=5)
            self.assertEqual(gw.balance("alice"), job_count * 10)
            self.assertEqual(gw.balance("bob"), job_count * 5)
            self.assertEqual(len(gw.committed_keys()), job_count * 2)
            self.assertEqual(len(set(gw.committed_keys())), job_count * 2)
            self.assertEqual(len(all_outcomes), job_count * 2)


if __name__ == "__main__":
    unittest.main()
