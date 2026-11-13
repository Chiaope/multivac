"""The types shared by the engine, the gateway and the tests. Do not modify."""
from dataclasses import dataclass

PENDING = "PENDING"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"

IDLE = "idle"
PROGRESS = "progress"
SUCCEEDED_OUTCOME = "succeeded"
RETRY = "retry"
FAILED_OUTCOME = "failed"

STAGES = ("after_claim", "before_effect", "after_effect", "before_finalize",
          "after_finalize")


@dataclass(frozen=True)
class Step:
    account: str
    amount: int


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    steps: tuple[Step, ...]


@dataclass(frozen=True)
class JobSnapshot:
    job_id: str
    status: str  # PENDING, RUNNING, SUCCEEDED or FAILED
    step_index: int  # the next unfinished step; the step count once SUCCEEDED
    step_count: int
    failures: int  # transient gateway errors on the current step
    ready_at: float | None  # earliest next attempt while waiting to retry, else None
    last_error: str | None  # the last gateway error on the current step, else None


@dataclass(frozen=True)
class TickOutcome:
    kind: str  # idle, progress, succeeded, retry or failed
    job_id: str | None = None
    step_index: int | None = None


class ConflictError(Exception):
    """A job ID was submitted again with different steps."""


class GatewayError(Exception):
    pass


class TransientGatewayError(GatewayError):
    """The call may or may not have taken effect; trying again may succeed."""


class ThrottledGatewayError(TransientGatewayError):
    """The gateway refused the call under its rate limit; nothing was done."""


class PermanentGatewayError(GatewayError):
    """The gateway refused the credit; trying again will not help."""


def stable_key(job_id: str, step_index: int) -> str:
    """The idempotency key of one logical step."""
    return f"{job_id}#{step_index}"
