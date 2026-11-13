"""Contract tests for the engine. Some fail against the supplied engine.

A crash is simulated by a hook that raises SystemExit: the engine object is
then abandoned and a new one is opened on the same file.
"""
import gc
import os
import tempfile
import unittest

from durableflow import (FAILED, PENDING, RUNNING, SUCCEEDED, ConflictError, Engine,
                         FakeClock, Gateway, JobSpec, Step, stable_key)


def crash_at(stage, job_id=None, step_index=None):
    """A hook that crashes the process at `stage` (of one job and step, or any)."""
    def hook(s, j, i):
        if s == stage and job_id in (None, j) and step_index in (None, i):
            raise SystemExit(f"crash at {stage}")
    return hook


def recorder(log):
    def hook(s, j, i):
        log.append((s, j, i))
    return hook


class Case(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.state = os.path.join(self.dir.name, "state.db")
        self.clock = FakeClock(1000.0)
        self.gateway = Gateway(os.path.join(self.dir.name, "gateway.db"), self.clock)

    def tearDown(self):
        gc.collect()
        self.dir.cleanup()

    def engine(self, **kw):
        return Engine(self.state, self.gateway, self.clock, **kw)

    def crash(self, engine, stage, worker="w1"):
        with self.assertRaises(SystemExit):
            engine.tick(worker, hook=crash_at(stage))
        del engine
        gc.collect()

    def run_until_idle(self, engine, worker="w1", limit=100):
        outcomes = []
        for _ in range(limit):
            out = engine.tick(worker)
            if out.kind == "idle":
                return outcomes
            outcomes.append(out)
        self.fail("engine never went idle")


class Submit(Case):
    def test_new_and_identical(self):
        e = self.engine()
        spec = JobSpec("job-1", (Step("alice", 10), Step("bob", 20)))
        self.assertTrue(e.submit(spec))
        self.assertFalse(e.submit(spec))

    def test_conflicting_steps(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10),)))
        with self.assertRaises(ConflictError):
            e.submit(JobSpec("job-1", (Step("alice", 11),)))

    def test_empty_job_succeeds_at_once(self):
        e = self.engine()
        self.assertTrue(e.submit(JobSpec("job-0", ())))
        snap = e.get("job-0")
        self.assertEqual((snap.status, snap.step_index), (SUCCEEDED, 0))
        self.assertEqual(e.tick("w1").kind, "idle")

    def test_unknown_job(self):
        self.assertIsNone(self.engine().get("nope"))


class HappyPath(Case):
    def test_two_steps(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 20))))
        outs = self.run_until_idle(e)
        self.assertEqual([(o.kind, o.job_id, o.step_index) for o in outs],
                         [("progress", "job-1", 0), ("succeeded", "job-1", 1)])
        self.assertEqual((self.gateway.balance("alice"), self.gateway.balance("bob")),
                         (10, 20))
        snap = e.get("job-1")
        self.assertEqual((snap.status, snap.step_index, snap.failures), (SUCCEEDED, 2, 0))

    def test_hooks_in_order(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10),)))
        log = []
        e.tick("w1", hook=recorder(log))
        self.assertEqual([s for s, _, _ in log], ["after_claim", "before_effect",
                                                   "after_effect", "before_finalize",
                                                   "after_finalize"])


class Retries(Case):
    def test_retry_waits_for_ready_at(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10),)))
        self.gateway.set_plan(stable_key("job-1", 0), ["before", "before"])
        self.assertEqual(e.tick("w1").kind, "retry")
        snap = e.get("job-1")
        self.assertEqual((snap.status, snap.failures), (PENDING, 1))
        self.assertGreater(snap.ready_at, 1000.0)  # never at once
        self.assertEqual(e.tick("w1").kind, "idle")  # not ready yet
        self.clock.set(snap.ready_at)
        self.assertEqual(e.tick("w1").kind, "retry")
        self.clock.set(e.get("job-1").ready_at)
        self.assertEqual(e.tick("w1").kind, "succeeded")
        self.assertEqual(self.gateway.balance("alice"), 10)

    def test_error_hooks_bracket_finalize(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10),)))
        self.gateway.set_plan(stable_key("job-1", 0), ["before"])
        log = []
        e.tick("w1", hook=recorder(log))
        self.assertEqual([s for s, _, _ in log], ["after_claim", "before_effect",
                                                   "before_finalize", "after_finalize"])

    def test_max_failures(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 5))))
        self.gateway.set_plan(stable_key("job-1", 0), ["before"] * 3)
        kinds = []
        for _ in range(3):
            self.clock.advance(100)
            kinds.append(e.tick("w1").kind)
        self.assertEqual(kinds, ["retry", "retry", "failed"])
        snap = e.get("job-1")
        self.assertEqual((snap.status, snap.step_index, snap.failures), (FAILED, 0, 3))
        self.assertEqual(self.gateway.calls(stable_key("job-1", 1)), [])

    def test_failures_reset_after_a_step(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 5))))
        self.gateway.set_plan(stable_key("job-1", 0), ["before", "before"])
        self.gateway.set_plan(stable_key("job-1", 1), ["before", "before"])
        for _ in range(6):
            self.clock.advance(100)
            e.tick("w1")
        self.assertEqual(e.get("job-1").status, SUCCEEDED)

    def test_permanent_rejection_stops_the_job(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 5), Step("carol", 1))))
        self.gateway.set_plan(stable_key("job-1", 1), ["reject"])
        self.run_until_idle(e)
        snap = e.get("job-1")
        self.assertEqual((snap.status, snap.step_index), (FAILED, 1))
        self.assertEqual(self.gateway.balance("alice"), 10)  # not rolled back
        self.assertEqual(self.gateway.calls(stable_key("job-1", 2)), [])

    def test_lost_acknowledgment_credits_once(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10),)))
        self.gateway.set_plan(stable_key("job-1", 0), ["after", "ok"])
        self.assertEqual(e.tick("w1").kind, "retry")
        self.clock.set(e.get("job-1").ready_at)
        self.assertEqual(e.tick("w1").kind, "succeeded")
        self.assertEqual(self.gateway.balance("alice"), 10)


class Recovery(Case):
    def test_crash_after_effect_resumes_after_lease(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 20))))
        self.crash(e, "after_effect")
        e2 = self.engine()
        self.assertEqual(e2.get("job-1").status, RUNNING)
        self.assertEqual(e2.tick("w2").kind, "idle")  # the lease still holds
        self.clock.advance(10)
        self.assertEqual(e2.tick("w2").kind, "progress")
        self.run_until_idle(e2, "w2")
        self.assertEqual(e2.get("job-1").status, SUCCEEDED)
        self.assertEqual((self.gateway.balance("alice"), self.gateway.balance("bob")),
                         (10, 20))

    def test_state_survives_restart(self):
        e = self.engine()
        e.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 20))))
        self.gateway.set_plan(stable_key("job-1", 1), ["before"])
        e.tick("w1")
        e.tick("w1")
        del e
        gc.collect()
        snap = self.engine().get("job-1")
        self.assertEqual((snap.status, snap.step_index, snap.failures),
                         (PENDING, 1, 1))
        self.assertGreater(snap.ready_at, 1000.0)
        self.assertIn("job-1#1", snap.last_error)


class Claims(Case):
    def test_stale_worker_does_not_overwrite(self):
        e1, e2 = self.engine(), self.engine()
        e1.submit(JobSpec("job-1", (Step("alice", 10), Step("bob", 20))))
        seen = []

        def overtaken(stage, job_id, step_index):
            if stage == "before_effect":
                self.clock.advance(11)  # w1's lease runs out
                seen.append(e2.tick("w2"))  # w2 claims and completes step 0
        out = e1.tick("w1", hook=overtaken)
        self.assertEqual(seen[0].kind, "progress")
        self.assertEqual(out.kind, "idle")  # w1 must not record its result
        snap = e1.get("job-1")
        self.assertEqual((snap.status, snap.step_index), (PENDING, 1))
        self.assertEqual(self.gateway.balance("alice"), 10)


if __name__ == "__main__":
    unittest.main()
