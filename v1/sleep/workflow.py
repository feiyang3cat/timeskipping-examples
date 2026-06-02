import asyncio
from datetime import timedelta

from temporalio import workflow

from activities import dummy_activity

# BEST PRACTICE OF SLEEP
@workflow.defn
class WorkflowNeedInteraction:
    """A workflow waiting on a signal-driven condition."""

    def __init__(self) -> None:
        self._signal_count = 0

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signal_count >= 1)
        return workflow.now().isoformat()

    @workflow.signal
    def go(self) -> None:
        self._signal_count += 1

@workflow.defn
class WorkflowNeedHasTimerAndInteraction:
    """Race a 1h timer against a `go` signal after an initial 1h sleep.

    Returns True if the signal arrived within 1 minute after the sleep, False otherwise.
    """

    def __init__(self) -> None:
        self._prepared = False
        self._ready = False

    @workflow.signal
    def go(self) -> None:
        if self._prepared:
            self._ready = True

    @workflow.run
    async def run(self) -> bool:
        await workflow.execute_activity(
            dummy_activity,
            "some prep activity",
            start_to_close_timeout=timedelta(seconds=60),
        )
        self._prepared = True
        await workflow.sleep(timedelta(hours=1))
        try:
            await workflow.wait_condition(
                lambda: self._ready, timeout=timedelta(minutes=1)
            )
        except asyncio.TimeoutError:
            pass
        return self._ready


@workflow.defn
class ThisWorkflowRunsWithCronOrRetry:
    """A short workflow run — 30s timer then records completion — used as a cron body.

    Note: the test server uses UNIX 5-field cron (minute granularity is the floor),
    so the original 1s/500ms/3s scenario is scaled up 60× to 1min/30s/3min.
    """
    @workflow.run
    async def run(self):
        await workflow.execute_activity(
            dummy_activity,
            "routine activity for cron",
            start_to_close_timeout=timedelta(seconds=10),
        )


# ANTI-PATTERNS:
@workflow.defn
class BusyWorkflow:
    """Don't use sleep."""
    """A busy workflow with no waiting point — time-skipping has nothing to skip."""
    """In v1, sleep will wait until the testing server time passes to the sleep point."""
    """But in v2, time is per-execution; sleep may return an error when the execution (not a single run) completes and time hasn't reached the sleep point."""

    @workflow.run
    async def run(self, seconds_each: float) -> str:
        for _ in range(3):
            await workflow.execute_activity(
                dummy_activity,
                "busy_workflow_activity",
                start_to_close_timeout=timedelta(seconds=60),
            )
        return workflow.now().isoformat()

@workflow.defn
class WaitingWorkflowOnUserTimer:
    """Don't use sleep."""
    """Just enable time skipping and let it run to completion."""

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(days=10))
        return "done"

