import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from activities import dummy_activity, retry_activity

default_retry_policy = RetryPolicy(maximum_attempts=1)


# --- Best practices ---

@workflow.defn
class WorkflowWithUserTimer:
    @workflow.run
    async def run(self, wait_hours: int = 1) -> str:
        await workflow.execute_activity(
            dummy_activity,
            "preparation activity",
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=default_retry_policy,
        )
        await workflow.sleep(timedelta(hours=wait_hours))
        await workflow.execute_activity(
            dummy_activity,
            "cleanup activity",
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=default_retry_policy,
        )
        return workflow.now().isoformat()


@workflow.defn
class ParentChildWorkflowWithUserTimer:
    @workflow.run
    async def run(self) -> str:
        child_wf_1_id = f"child-wf-1-{workflow.uuid4()}"
        child_wf_2_id = f"child-wf-2-{workflow.uuid4()}"
        child_wf_1_handle = await workflow.start_child_workflow(
            WorkflowWithUserTimer.run,
            1,  # wait_hours
            id=child_wf_1_id,
        )
        child_wf_2_handle = await workflow.start_child_workflow(
            WorkflowWithUserTimer.run,
            2,  # wait_hours
            id=child_wf_2_id,
        )
        await asyncio.gather(
            child_wf_1_handle,
            child_wf_2_handle,
        )
        return workflow.now().isoformat()


@workflow.defn
class WorkflowWithActivityRetries:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            retry_activity,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=2.0,
                maximum_attempts=3,
            ),
        )


@workflow.defn
class WorkflowNeedInteraction:
    """Waits on a signal-driven condition — use env.sleep() to pause time-skipping,
    then send the signal before the timeout fires."""

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
    """Races a 1h timer against a `go` signal after an initial 1h sleep.

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
    """Short workflow body used as a cron/retry target.

    Note: the test server uses UNIX 5-field cron (minute granularity is the floor),
    so sub-minute schedules must be scaled up 60× (e.g. 1s → 1min).
    """

    @workflow.run
    async def run(self):
        await workflow.execute_activity(
            dummy_activity,
            "routine activity for cron",
            start_to_close_timeout=timedelta(seconds=10),
        )


# --- Anti-patterns ---

@workflow.defn
class BusyWorkflow:
    """Anti-pattern: no sleep or wait_condition — time-skipping has nothing to skip.
    This workflow runs at real wall-clock speed regardless of the test environment.
    In v2, calling env.sleep() past the end of execution returns an error.
    """

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
    """Anti-pattern: workflow has a sleep but the test uses env.sleep() unnecessarily.
    Just let time-skipping run the workflow to completion on its own.
    """

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(days=10))
        return "done"
