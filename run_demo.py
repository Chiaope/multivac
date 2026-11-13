"""A short run: two agent runs (jobs) whose tool calls credit accounts, with one
lost acknowledgment, one crash and one restart.

    python run_demo.py
"""
import gc
import os
import tempfile

from durableflow import Engine, FakeClock, Gateway, JobSpec, Step, stable_key


def main():
    with tempfile.TemporaryDirectory() as d:
        clock = FakeClock(0.0)
        gateway = Gateway(os.path.join(d, "gateway.db"), clock)
        state = os.path.join(d, "state.db")
        engine = Engine(state, gateway, clock)
        engine.submit(JobSpec("pay-1", (Step("alice", 10), Step("bob", 20))))
        engine.submit(JobSpec("pay-2", (Step("carol", 5),)))
        gateway.set_plan(stable_key("pay-2", 0), ["after", "ok"])

        def crash_once(stage, job_id, step_index):
            if (stage, job_id, step_index) == ("after_effect", "pay-1", 1):
                raise SystemExit("crash")

        for _ in range(3):
            try:
                print(engine.tick("worker-a", hook=crash_once))
            except SystemExit:
                print("worker-a crashed after crediting bob")
                del engine
                gc.collect()
                engine = Engine(state, gateway, clock)
                break
        for _ in range(10):
            clock.advance(5)
            out = engine.tick("worker-b")
            if out.kind != "idle":
                print(out)
        for job in ("pay-1", "pay-2"):
            print(engine.get(job))
        print({a: gateway.balance(a) for a in ("alice", "bob", "carol")})


if __name__ == "__main__":
    main()
