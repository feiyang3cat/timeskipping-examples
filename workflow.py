import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from activities import dummy_activity, retry_activity

default_retry_policy = RetryPolicy(maximum_attempts=1)


# --- Scenario 1: User Timers ---

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


# --- Scenario 2: Workflow Retry ---

@workflow.defn
class FailFirstAttemptWorkflow:
    @workflow.run
    async def run(self) -> str:
        if workflow.info().attempt == 1:
            raise ApplicationError("first attempt fails")
        return f"succeeded on attempt {workflow.info().attempt}"


# --- Scenario 3: Workflow Execution/Run Timeouts ---

@workflow.defn
class WaitForSignalWorkflow:
    def __init__(self) -> None:
        self._signaled = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signaled)
        return "signaled"

    @workflow.signal
    def go(self) -> None:
        self._signaled = True


# --- Scenario 4: Workflow Start Delay ---

@workflow.defn
class WorkflowWithStartDelay:
    @workflow.run
    async def run(self) -> str:
        return workflow.now().isoformat()


# --- Scenario 5: Activity Retry ---

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


# --- Scenario 6: Cron ---

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


# --- Scenario 7: Scheduler --- (not supported in v1)

# --- Scenario 8: SAA --- (not supported in v1)

# --- Scenario 9: TimeSkipping Sleep ---

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
class TwoTimerWorkflow:
    """Two concurrent timers (1h and 10h) — used to test partial clock advances."""

    def __init__(self) -> None:
        self._short_fired = False
        self._long_fired = False

    @workflow.run
    async def run(self) -> None:
        async def short_timer() -> None:
            await workflow.sleep(timedelta(hours=1))
            self._short_fired = True

        async def long_timer() -> None:
            await workflow.sleep(timedelta(hours=10))
            self._long_fired = True

        await asyncio.gather(short_timer(), long_timer())

    @workflow.query
    def short_fired(self) -> bool:
        return self._short_fired

    @workflow.query
    def long_fired(self) -> bool:
        return self._long_fired


@dataclass
class ParentChildResult:
    end: str
    parent_signal_won: bool
    child_signal_won: bool


@workflow.defn
class ChildWorkflowWaitingForSignal:
    """Child workflow: sleeps 30min, then races a 5min timer against a `go` signal."""

    APPROVAL_TIMEOUT = timedelta(minutes=5)

    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def go(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self) -> bool:
        await workflow.sleep(timedelta(minutes=30))
        try:
            await workflow.wait_condition(
                lambda: self._signaled, timeout=self.APPROVAL_TIMEOUT
            )
            return True
        except asyncio.TimeoutError:
            return False


@workflow.defn
class ParentWorkflowWithChildInteraction:
    """Parent: sleeps 1h → starts child → sleeps 1h → races 5min timer vs signal.

    Used to demonstrate env.sleep() stepping through a multi-workflow interaction.
    """

    APPROVAL_TIMEOUT = timedelta(minutes=5)

    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def go(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self, child_id: str) -> ParentChildResult:
        await workflow.execute_activity(
            dummy_activity,
            "sending a notification",
            start_to_close_timeout=timedelta(seconds=5),
        )
        await workflow.sleep(timedelta(hours=1))

        child_signal_won = await workflow.execute_child_workflow(
            ChildWorkflowWaitingForSignal.run,
            id=child_id,
            task_queue=workflow.info().task_queue,
        )

        await workflow.sleep(timedelta(hours=1))

        try:
            await workflow.wait_condition(
                lambda: self._signaled, timeout=self.APPROVAL_TIMEOUT
            )
            parent_signal_won = True
        except asyncio.TimeoutError:
            parent_signal_won = False

        return ParentChildResult(
            end=workflow.now().isoformat(),
            parent_signal_won=parent_signal_won,
            child_signal_won=child_signal_won,
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
