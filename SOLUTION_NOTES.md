# DurableFlow Solution Notes

## What the logs showed
Reviewing `logs/oncall.md`, `logs/engine.jsonl`, and `logs/gateway.jsonl` surfaced four production failure modes:
1. **Duplicate payouts after key expiry (45 keys applied twice in `gateway.jsonl`):** `gateway.py` only remembers idempotency keys for `key_ttl` (300s default). Retries or crash recoveries after that window paid again.
2. **Stale workers and shared worker IDs:** On Oct 13, `app-2` paused and resumed to double-pay. Also, `app-1`, `app-3`, and `app-7` all ran under `worker-1`, so worker IDs are not unique.
3. **Thundering herd and 429 failures:** On Oct 7–8 and Oct 23, synchronized retries triggered rate limiting (666 `throttled` / 429 responses) and failed jobs.
4. **Windows file-lock / long-lived connection:** Keeping a single SQLite connection open on `Engine` caused `PermissionError` during `tempfile.TemporaryDirectory` cleanup in `run_demo.py` and risked holding locks across hooks.

## Design (`durableflow/engine.py`)
Each `tick()` runs as three short SQLite transactions (`BEGIN IMMEDIATE` with WAL mode and per-call `_Closing` connections), never holding a DB lock during hooks or gateway calls:
1. **Claim:** Atomically selects the earliest-submitted (`seq`) eligible job (`status IN ('PENDING', 'RUNNING')`, expired or null lease, `ready_at <= now`), sets `lease_until = now + lease_seconds`, increments `attempts`, and bumps a monotonic `token` (fencing token).
2. **Confirm & Effect:** After `after_claim` and `before_effect` hooks, `_confirm` renews the lease only if `token` still matches; otherwise the stale worker returns `idle`. If `attempts > 1` (prior crash, takeover, or retry), the engine calls `gateway.lookup(key)` first and skips `credit()` if a receipt already exists.
3. **Finalize:** `_record_done` or `_record_error` writes only if `token` and `step_index` still match, clearing step retry state on success or computing `ready_at` via capped exponential backoff (`retry_base_seconds * 2^(n-1)`, capped at `30 * retry_base_seconds`) with deterministic SHA-256 jitter in `[0.5, 1.0]`.

## Assumptions
- **Throttling (`ThrottledGatewayError`):** Treated as a rate-limit refusal rather than a step failure—it schedules a backoff retry (`throttles += 1`) without incrementing `failures` toward `max_failures`, matching the Oct 23 incident goal.
- **Lease renewal at confirm:** `_confirm` renews the lease if `token` still matches, even if the deadline passed while no other worker claimed the job.
- **Finalize hooks on takeover:** `before_finalize` and `after_finalize` still run even if the finalize transaction finds the claim was overtaken.

## Commands and results
- `.\.venv\Scripts\python.exe run_demo.py` — Exits 0; `pay-1` and `pay-2` both `SUCCEEDED` with balances `{'alice': 10, 'bob': 20, 'carol': 5}`.
- `.\.venv\Scripts\python.exe -m unittest discover -s tests -v` — Ran 28 tests (15 contract tests + 13 tests in `tests/test_durableflow.py` covering stage crashes, `key_ttl` expiry, `lookup` failures, lease boundaries, fencing with duplicate `worker_id`, 10-minute outage, hook DB access, and 2-process `spawn` racing) in 4.617s: `OK`.

## Revisions and unresolved items
- Restored `ready_at <= ?` (from `<`) in `_claim` after 5 tests caught off-by-one eligibility at the exact `ready_at` timestamp.
- **Unresolved / trade-off:** Confirm whether `ThrottledGatewayError` should count toward `max_failures` under strict reading of Section 3 vs. the Oct 23 on-call note; currently it does not.
