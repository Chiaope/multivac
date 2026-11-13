"""The remote side: a payment gateway with its own database. Do not modify.

The engine may call `credit`, `lookup` and `balance`. The other methods
configure faults and inspect what happened, for tests.

Idempotency keys. A credit carries a key. While the gateway remembers a key,
a second credit with the same key and request returns the first receipt and
credits nothing. The gateway remembers a key for less than `key_ttl` seconds
after the credit that used it (KEY_TTL_SECONDS unless the Gateway was made
with another value); a credit with the same key `key_ttl` seconds or more
later is a new credit.

Lookup. `lookup(key)` reports whether a credit with this key was ever
applied, however long ago: the receipt, or None. A lookup can fail.

Rate limit. A gateway made with `rate_limit` refuses calls beyond that many in
any one second, with ThrottledGatewayError (a transient error, nothing done).
Refused calls count against the limit too.

Plans script the next calls for one key, for tests:

  set_plan(key, actions)          for credit: before, after, reject, ok
  set_lookup_plan(key, actions)   for lookup: fail, ok
  set_outage(start, end)          every call in [start, end) fails, transiently

  before  a transient error before anything is committed
  after   the credit is applied, then a transient error: the acknowledgment
          is lost
  reject  a permanent rejection; nothing is credited
  ok      success, or the first receipt while the key is remembered
  fail    (lookup) a transient error; nothing is reported

With no action left, a call behaves as "ok". Plans and keys are durable. The
gateway's clock is the one it was given.
"""
import json
import sqlite3

from .model import PermanentGatewayError, ThrottledGatewayError, TransientGatewayError

CREDIT_ACTIONS = ("before", "after", "reject", "ok")
LOOKUP_ACTIONS = ("fail", "ok")
KEY_TTL_SECONDS = 300.0
BUSY_TIMEOUT_S = 30


class Gateway:
    def __init__(self, path: str, clock, key_ttl: float = KEY_TTL_SECONDS,
                 rate_limit: int | None = None):
        self.path = path
        self.clock = clock
        self.key_ttl = key_ttl
        self.rate_limit = rate_limit
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY, account TEXT NOT NULL,
                    amount INTEGER NOT NULL, receipt TEXT NOT NULL, at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS ledger (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL,
                    account TEXT NOT NULL, amount INTEGER NOT NULL,
                    receipt TEXT NOT NULL, at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS balances (
                    account TEXT PRIMARY KEY, balance INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS plans (
                    key TEXT NOT NULL, kind TEXT NOT NULL, actions TEXT NOT NULL,
                    PRIMARY KEY (key, kind));
                CREATE TABLE IF NOT EXISTS outages (start REAL NOT NULL, end REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS calls (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL,
                    kind TEXT NOT NULL, key TEXT NOT NULL, account TEXT,
                    amount INTEGER, action TEXT NOT NULL, result TEXT NOT NULL);
            """)

    def _db(self):
        db = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_S, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        return _Closing(db)

    # ------------------------------------------------------------ public API
    def credit(self, account: str, amount: int, key: str) -> str:
        """Credit `amount` to `account`; return the receipt."""
        now = self.clock.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            refusal = self._refused(db, now, "credit", key, account, amount)
            if refusal:
                db.execute("COMMIT")
                raise refusal
            action = self._next_action(db, key, "credit")
            row = db.execute("SELECT account, amount, receipt, at FROM keys WHERE key = ?",
                             (key,)).fetchone()
            remembered = row is not None and now - row[3] < self.key_ttl
            if remembered and (row[0], row[1]) != (account, amount):
                self._log(db, now, "credit", key, account, amount, action, "conflict")
                db.execute("COMMIT")
                raise PermanentGatewayError(f"key {key} was used for another request")
            if action == "before":
                self._log(db, now, "credit", key, account, amount, action, "transient")
                db.execute("COMMIT")
                raise TransientGatewayError(f"timeout before commit ({key})")
            if action == "reject":
                self._log(db, now, "credit", key, account, amount, action, "rejected")
                db.execute("COMMIT")
                raise PermanentGatewayError(f"credit rejected ({key})")
            if remembered:
                receipt, result = row[2], "replayed"
            else:
                n = db.execute("SELECT COUNT(*) FROM ledger").fetchone()[0] + 1
                receipt = f"rcpt-{n}"
                db.execute("INSERT OR REPLACE INTO keys VALUES (?, ?, ?, ?, ?)",
                           (key, account, amount, receipt, now))
                db.execute("INSERT INTO ledger (key, account, amount, receipt, at) "
                           "VALUES (?, ?, ?, ?, ?)", (key, account, amount, receipt, now))
                db.execute("INSERT INTO balances VALUES (?, ?) ON CONFLICT(account) DO "
                           "UPDATE SET balance = balance + excluded.balance",
                           (account, amount))
                result = "committed"
            self._log(db, now, "credit", key, account, amount, action, result)
            db.execute("COMMIT")
        if action == "after":
            raise TransientGatewayError(f"acknowledgment lost ({key})")
        return receipt

    def lookup(self, key: str) -> str | None:
        """The receipt of the last credit applied with `key`, ever; None if none."""
        now = self.clock.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            refusal = self._refused(db, now, "lookup", key, None, None)
            if refusal:
                db.execute("COMMIT")
                raise refusal
            action = self._next_action(db, key, "lookup")
            if action == "fail":
                self._log(db, now, "lookup", key, None, None, action, "transient")
                db.execute("COMMIT")
                raise TransientGatewayError(f"lookup failed ({key})")
            row = db.execute("SELECT receipt FROM ledger WHERE key = ? ORDER BY seq DESC "
                             "LIMIT 1", (key,)).fetchone()
            self._log(db, now, "lookup", key, None, None, action,
                      "found" if row else "absent")
            db.execute("COMMIT")
        return row[0] if row else None

    def balance(self, account: str) -> int:
        with self._db() as db:
            row = db.execute("SELECT balance FROM balances WHERE account = ?",
                             (account,)).fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------ for tests
    def set_plan(self, key: str, actions: list[str]) -> None:
        """Script the next credit calls for `key`, replacing any plan left."""
        self._set_plan(key, "credit", actions, CREDIT_ACTIONS)

    def set_lookup_plan(self, key: str, actions: list[str]) -> None:
        """Script the next lookup calls for `key`, replacing any plan left."""
        self._set_plan(key, "lookup", actions, LOOKUP_ACTIONS)

    def set_outage(self, start: float, end: float) -> None:
        """Every call made while start <= now < end fails, transiently."""
        with self._db() as db:
            db.execute("INSERT INTO outages VALUES (?, ?)", (start, end))

    def calls(self, key: str | None = None, kind: str = "credit") -> list[dict]:
        """Calls of one kind (credit or lookup), oldest first, for `key` or all."""
        q = "SELECT seq, at, key, account, amount, action, result FROM calls WHERE kind = ?"
        args = [kind]
        if key:
            q += " AND key = ?"
            args.append(key)
        with self._db() as db:
            rows = db.execute(q + " ORDER BY seq", args).fetchall()
        return [dict(zip(("seq", "at", "key", "account", "amount", "action", "result"), r))
                for r in rows]

    def committed_keys(self) -> list[str]:
        """The key of every credit applied, in order; a key twice means paid twice."""
        with self._db() as db:
            return [r[0] for r in db.execute("SELECT key FROM ledger ORDER BY seq")]

    # ------------------------------------------------------------ internals
    def _set_plan(self, key, kind, actions, allowed):
        bad = [a for a in actions if a not in allowed]
        if bad:
            raise ValueError(f"unknown actions {bad}; use {allowed}")
        with self._db() as db:
            db.execute("INSERT INTO plans VALUES (?, ?, ?) ON CONFLICT(key, kind) DO UPDATE "
                       "SET actions = excluded.actions", (key, kind, json.dumps(actions)))

    def _refused(self, db, now, kind, key, account, amount):
        """An outage or the rate limit, as the error to raise; logged."""
        if db.execute("SELECT 1 FROM outages WHERE start <= ? AND ? < end",
                      (now, now)).fetchone():
            self._log(db, now, kind, key, account, amount, "-", "unavailable")
            return TransientGatewayError(f"gateway unavailable ({key})")
        if self.rate_limit is not None:
            recent = db.execute("SELECT COUNT(*) FROM calls WHERE at > ? AND at <= ?",
                                (now - 1.0, now)).fetchone()[0]
            if recent >= self.rate_limit:
                self._log(db, now, kind, key, account, amount, "-", "throttled")
                return ThrottledGatewayError(f"rate limit exceeded ({key})")
        return None

    @staticmethod
    def _next_action(db, key, kind):
        row = db.execute("SELECT actions FROM plans WHERE key = ? AND kind = ?",
                         (key, kind)).fetchone()
        if row is None:
            return "ok"
        actions = json.loads(row[0])
        if not actions:
            return "ok"
        db.execute("UPDATE plans SET actions = ? WHERE key = ? AND kind = ?",
                   (json.dumps(actions[1:]), key, kind))
        return actions[0]

    @staticmethod
    def _log(db, now, kind, key, account, amount, action, result):
        db.execute("INSERT INTO calls (at, kind, key, account, amount, action, result) "
                   "VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (now, kind, key, account, amount, action, result))


class _Closing:
    """A connection that is closed when the block ends; an open transaction
    is rolled back."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, *exc):
        if self.db.in_transaction:
            self.db.execute("ROLLBACK")
        self.db.close()
