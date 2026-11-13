# On-call notes, payflow (v0)

Kept by whoever was on call. Times are UTC.

**Oct 7, 02:10-02:24.** Gateway outage, their status page confirms. Most runs
in flight failed ("503 unavailable", three failures each). At 02:30 we resubmitted every
failed run by hand. Gateway team called us afterwards: they saw a burst from
us well above their per-client limit and started refusing. They asked us to
"be nicer" next time.

**Oct 8.** Changed the retry rule: a failed call now waits until the next full
minute instead of 2 seconds, so we don't hammer them.

**Oct 13, 14:02.** app-2 stopped heartbeating; came back at 14:11 after the
hypervisor restart. Runs resumed on their own, nothing failed. Oct 15:
finance flagged double payouts dated Oct 13, all from app-2, refunded by hand.
Cause not found.

**Oct 20.** Customer support: some runs show "done" but one recipient was paid
twice and another payment came in minutes late. All on app-3 or app-7, both
added last month from the same image.

**Oct 23, 03:40.** Gateway blip, under a minute. Most runs recovered, but the
gateway throttled us around it and a lot of runs failed with 429s. Nobody
changed anything.
