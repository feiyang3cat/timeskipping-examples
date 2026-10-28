import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

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
# (uses ThisWorkflowRunsWithCronOrRetry from Scenario 6 with a mocked failing activity)

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
    async def run(self) -> str:
        await workflow.execute_activity(
            retry_activity,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(hours=1),
                backoff_coefficient=2.0,
                maximum_attempts=3,
            ),
        )
        return workflow.now().isoformat()


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
class WorkflowWaitingForSignals:
    _waiting_timeout = timedelta(minutes=10)

    def __init__(self) -> None:
        self._prepared = False
        self._signaled = False

    @workflow.signal
    def go(self) -> None:
        if self._prepared:
            self._signaled = True

    @workflow.signal
    def prepare_done(self) -> None:
        self._prepared = True

    @workflow.run
    async def run(self) -> bool:
        await workflow.sleep(timedelta(hours=1))
        try:
            await workflow.wait_condition(
                lambda: self._signaled, timeout=self._waiting_timeout
            )
            return True
        except asyncio.TimeoutError:
            return False

# --- Scenario 9+: complicated examples for sleep ---

@workflow.defn
class ChildA:
    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(hours=1))
        return workflow.now().isoformat()


@workflow.defn
class ChildB:
    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(hours=2))
        return workflow.now().isoformat()


@workflow.defn
class ParentWithChildAndWaitCondition:
    def __init__(self) -> None:
        self._approved = False

    @workflow.signal
    def approve(self) -> None:
        self._approved = True

    @workflow.run
    async def run(self) -> bool:
        parent_id = workflow.info().workflow_id
        handle_a, handle_b = await asyncio.gather(
            workflow.start_child_workflow(ChildA.run, id=f"{parent_id}-a"),
            workflow.start_child_workflow(ChildB.run, id=f"{parent_id}-b"),
        )
        # ChildA finishes at +1h, ChildB at +2h; parent unblocks at +2h
        await asyncio.gather(handle_a, handle_b)
        # Additional 1h cooldown before opening the approval window (+3h total)
        await workflow.sleep(timedelta(hours=1))
        try:
            await workflow.wait_condition(lambda: self._approved, timeout=timedelta(hours=1))
        except asyncio.TimeoutError:
            pass
        return self._approved


@workflow.defn
class ChildWithCondition:
    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def proceed(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self) -> bool:
        await workflow.sleep(timedelta(minutes=30))
        try:
            await workflow.wait_condition(lambda: self._signaled, timeout=timedelta(minutes=5))
        except asyncio.TimeoutError:
            pass
        return self._signaled


@workflow.defn
class ParentChildBothWaitOnCondition:
    def __init__(self) -> None:
        self._signaled = False

    @workflow.signal
    def proceed(self) -> None:
        self._signaled = True

    @workflow.run
    async def run(self) -> tuple[bool, bool]:
        await workflow.sleep(timedelta(hours=1))
        # Child ID is deterministic so the caller can build a handle from the DB-stored parent ID
        child_id = f"{workflow.info().workflow_id}-child"
        child_signaled = await workflow.execute_child_workflow(
            ChildWithCondition.run,
            id=child_id,
        )
        await workflow.sleep(timedelta(hours=1))
        try:
            await workflow.wait_condition(lambda: self._signaled, timeout=timedelta(minutes=5))
        except asyncio.TimeoutError:
            pass
        return child_signaled, self._signaled


# --- Scenario 9++: activity racing a user timer (time-skipping gotcha) ---

@workflow.defn
class WorkflowRacingActivityAndTimer:
    """Races an activity against a user timer, returning whichever wins.

    While an activity is in flight, the time-skipping server normally keeps its
    clock at the real wall-clock rate, so a user timer would only fire after
    that many *real* seconds pass — even if its duration is far below the
    activity's start_to_close_timeout. To fire the user timer first without
    waiting in real time, the (mocked) activity calls ``env.sleep()`` — a manual
    time skip that advances the virtual clock while the activity is still
    running. The activity also does a real ``asyncio.sleep`` so it stays in
    flight until the skipped-forward user timer fires and cancels it.
    """

    @workflow.run
    async def run(self, timer_seconds: float, activity_timeout_seconds: float) -> str:
        activity_task = asyncio.ensure_future(
            workflow.execute_activity(
                dummy_activity,
                "racing activity",
                start_to_close_timeout=timedelta(seconds=activity_timeout_seconds),
                retry_policy=default_retry_policy,
            )
        )
        timer_task = asyncio.ensure_future(asyncio.sleep(timer_seconds))
        done, pending = await workflow.wait(
            [activity_task, timer_task], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        return "timer" if timer_task in done else "activity"


# --- Scenario 9+++: env.sleep() while an activity is in flight ---

@workflow.defn
class WorkflowRunningLongActivity:
    """Runs a single long activity.

    Used to show that env.sleep() does NOT skip virtual time while an activity
    is in flight: an in-flight activity holds a time-skipping lock, so any
    env.sleep() overlapping it elapses in real wall-clock time instead.
    """

    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            dummy_activity,
            "long running activity",
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=default_retry_policy,
        )


# --- Scenario 9++++: two user timers + an activity that skips time twice ---

@workflow.defn
class WorkflowWithTwoTimersAndActivity:
    """Waits on two user timers (e.g. 1h and 2h) and one activity, returning the
    order in which the three complete.

    While the activity is in flight the server pins its clock to real wall-clock
    speed, so neither timer would fire for real hours. The mocked activity
    instead calls ``env.sleep()`` twice from inside itself: each call is a manual
    time skip that advances the virtual clock, and the two skips accumulate — the
    first fires the 1h timer, the second fires the 2h timer — all while the
    activity is still running.
    """

    @workflow.run
    async def run(
        self,
        timer1_seconds: float,
        timer2_seconds: float,
        activity_timeout_seconds: float,
    ) -> list:
        start = workflow.now()
        tasks = {
            "timer1": asyncio.ensure_future(asyncio.sleep(timer1_seconds)),
            "timer2": asyncio.ensure_future(asyncio.sleep(timer2_seconds)),
            "activity": asyncio.ensure_future(
                workflow.execute_activity(
                    dummy_activity,
                    "racing activity",
                    start_to_close_timeout=timedelta(seconds=activity_timeout_seconds),
                    retry_policy=default_retry_policy,
                )
            ),
        }
        name_by_task = {task: name for name, task in tasks.items()}
        completion_order = []
        pending = set(tasks.values())
        while pending:
            done, pending = await workflow.wait(
                list(pending), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                elapsed_hours = (workflow.now() - start).total_seconds() / 3600
                completion_order.append([name_by_task[task], round(elapsed_hours)])
        return completion_order


# --- Anti-patterns of Sleep ---

@workflow.defn
class BusyWorkflow:
    """Anti-pattern:  usually this kind of busy workflow doesn't need time skipping
    if we are not testing retries/cron/etc.
    """

    @workflow.run
    async def run(self, seconds_each: float) -> str:
        for _ in range(3):
            await workflow.execute_activity(
                dummy_activity,
                "busy_workflow_activity",
                start_to_close_timeout=timedelta(minutes=10),
            )
        return workflow.now().isoformat()


@workflow.defn
class WaitingWorkflowOnUserTimer:
    """Anti-pattern: this workflow doesn't need to stop time skipping 
    in the middle and we don't need to use sleep and just enable time skipping
    to the end.
    """

    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(days=10))
        return "done"
